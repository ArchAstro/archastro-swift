// Copyright (c) 2026 ArchAstro Inc. Licensed under the MIT License.
// See LICENSE for details.

// Runtime: Phoenix Channels client for the generated Platform SDK.
// This file is hand-maintained, not generated. Port of the Python SDK's
// archastro/phx_channel package; reconnect and rejoin follow
// @archastro/sdk phx_channel (socket.ts, channel.ts). Socket: PhxSocket.swift.
//
// Wire protocol: JSON arrays [join_ref, ref, topic, event, payload]

import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

/// Reply envelope for a channel push: `{"status": "ok"|"error", "response": …}`.
public struct ChannelReply: Sendable {
    public let status: String
    public let response: JSONValue

    public init(status: String, response: JSONValue) {
        self.status = status
        self.response = response
    }
}

/// Raised when a channel operation fails (join rejected, timeout, transport).
public enum ChannelError: Error, Sendable {
    case joinRejected(JSONValue)
    case alreadyJoined(String)
    case timeout(String)
    case notConnected(String)
    case transport(String)
}

/// A single Phoenix Channel subscription on a topic.
public final class Channel: @unchecked Sendable {
    public enum State: String, Sendable {
        case closed, joining, joined, leaving, errored
    }

    /// Weak: the socket owns its channels; a channel must not keep a
    /// dropped socket (and its reconnect loop) alive.
    private weak var socket: Socket?
    public let topic: String

    private struct Internals {
        var state: Channel.State = .closed
        var joinRef: String?
        var joinPayload: JSONValue = .object([:])
        /// The join in flight. A second join() or a reconnect's rejoin()
        /// awaits it instead of sending another phx_join.
        var inFlightJoin: Task<JSONValue, any Error>?
        var pendingReplies: [String: CheckedContinuation<JSONValue, any Error>] = [:]
        var eventHandlers: [String: [(id: Int, callback: @Sendable (JSONValue) -> Void)]] = [:]
        var handlerCounter = 0
        // Inbound pushes that arrived before a handler was registered —
        // replayed to the first handler so autoPush-on-join can't be
        // missed. Bounded per event.
        var pendingPushes: [String: [JSONValue]] = [:]
    }

    /// Delivered to `on(Channel.rejoinedEvent)` with the join response each
    /// time the socket rejoins this channel after a reconnect. Every join
    /// response is the server's authoritative snapshot for the topic.
    public static let rejoinedEvent = "phx_rejoined"

    private let internals = Locked(Internals())
    private let pendingPushCap = 32

    init(socket: Socket, topic: String) {
        self.socket = socket
        self.topic = topic
    }

    public var state: State {
        internals.withLock { $0.state }
    }

    public var isJoined: Bool { state == .joined }

    private func liveSocket() throws -> Socket {
        guard let socket else { throw ChannelError.notConnected("Socket was released") }
        return socket
    }

    // MARK: Join / leave

    /// Join the channel; returns the server's join response payload.
    /// Throws `ChannelError` if the join is rejected, times out, or the
    /// channel is already joined. While a join is in flight, a second call
    /// awaits that join.
    public func join(
        _ payload: JSONValue? = nil,
        timeout: TimeInterval? = nil
    ) async throws -> JSONValue {
        let socket = try liveSocket()
        let effectiveTimeout = timeout ?? socket.timeout
        let ref = socket.makeRef()
        let task = try internals.withLock { state -> Task<JSONValue, any Error> in
            if let inFlight = state.inFlightJoin { return inFlight }
            if state.state == .joined {
                throw ChannelError.alreadyJoined(
                    "Channel \(topic) is already joined; call leave() first"
                )
            }
            if let payload { state.joinPayload = payload }
            return startJoin(&state, ref: ref, timeout: effectiveTimeout, rejoin: false)
        }
        return try await task.value
    }

    /// Rejoin with the last join payload after the socket reconnects, as
    /// @archastro/sdk Channel.rejoin does. A closed or leaving channel stays
    /// put; a failed rejoin leaves the channel errored. The eligibility
    /// check and the move to `.joining` share one lock section, so a
    /// concurrent leave() wins cleanly.
    func rejoin() async {
        guard let socket else { return }
        let ref = socket.makeRef()
        let timeout = socket.timeout
        let task = internals.withLock { state -> Task<JSONValue, any Error>? in
            if let inFlight = state.inFlightJoin { return inFlight }
            guard state.state == .joined || state.state == .errored else { return nil }
            return startJoin(&state, ref: ref, timeout: timeout, rejoin: true)
        }
        _ = try? await task?.value
    }

    /// Under the internals lock: mark joining and start the phx_join.
    private func startJoin(
        _ state: inout Internals,
        ref: String,
        timeout: TimeInterval,
        rejoin: Bool
    ) -> Task<JSONValue, any Error> {
        state.state = .joining
        state.joinRef = ref
        let payload = state.joinPayload
        let task = Task { [self] in
            try await performJoin(ref: ref, payload: payload, timeout: timeout, rejoin: rejoin)
        }
        state.inFlightJoin = task
        return task
    }

    private func performJoin(
        ref: String,
        payload: JSONValue,
        timeout: TimeInterval,
        rejoin: Bool
    ) async throws -> JSONValue {
        // Only the join that is still current may move the state.
        func settle(_ next: Channel.State) -> Bool {
            internals.withLock { state in
                guard state.joinRef == ref else { return false }
                state.inFlightJoin = nil
                if state.state == .joining { state.state = next }
                return state.state == next
            }
        }
        let envelope: JSONValue
        do {
            envelope = try await sendAndAwaitReply(
                joinRef: ref, ref: ref, event: "phx_join",
                payload: payload,
                timeout: timeout,
                timeoutMessage: "Join timed out for \(topic)"
            )
        } catch {
            _ = settle(.errored)
            throw error
        }
        guard envelope["status"]?.stringValue == "ok" else {
            _ = settle(.errored)
            throw ChannelError.joinRejected(envelope["response"] ?? .null)
        }
        let response = envelope["response"] ?? .object([:])
        if settle(.joined), rejoin {
            trigger(event: Channel.rejoinedEvent, payload: response)
        }
        return response
    }

    /// Leave the channel (best-effort — a timeout is swallowed).
    public func leave(timeout: TimeInterval? = nil) async throws {
        let proceed = internals.withLock { state -> Bool in
            guard state.state != .closed else { return false }
            state.state = .leaving
            state.inFlightJoin = nil
            return true
        }
        guard proceed else { return }
        guard let socket else {
            finishLeave()
            return
        }

        let ref = socket.makeRef()
        let currentJoinRef = internals.withLock { $0.joinRef }
        do {
            _ = try await sendAndAwaitReply(
                joinRef: currentJoinRef, ref: ref, event: "phx_leave",
                payload: .object([:]),
                timeout: timeout ?? socket.timeout,
                timeoutMessage: "Leave timed out for \(topic)"
            )
        } catch let error as ChannelError {
            if case .timeout = error {
                // Leave is best-effort.
            } else {
                finishLeave()
                throw error
            }
        }
        finishLeave()
    }

    private func finishLeave() {
        internals.withLock { state in
            state.state = .closed
            state.joinRef = nil
        }
        socket?.removeChannel(topic)
    }

    // MARK: Push / reply

    /// Push an event and await the reply envelope `{status, response}`.
    public func push(
        _ event: String,
        payload: some Encodable & Sendable,
        timeout: TimeInterval? = nil
    ) async throws -> ChannelReply {
        let payloadJSON = try JSONValue(encodable: payload)
        let socket = try liveSocket()
        let ref = socket.makeRef()
        let currentJoinRef = internals.withLock { $0.joinRef }
        let envelope = try await sendAndAwaitReply(
            joinRef: currentJoinRef, ref: ref, event: event,
            payload: payloadJSON,
            timeout: timeout ?? socket.timeout,
            timeoutMessage: "Push '\(event)' timed out on \(topic)"
        )
        return ChannelReply(
            status: envelope["status"]?.stringValue ?? "error",
            response: envelope["response"] ?? .null
        )
    }

    private func sendAndAwaitReply(
        joinRef: String?,
        ref: String,
        event: String,
        payload: JSONValue,
        timeout: TimeInterval,
        timeoutMessage: String
    ) async throws -> JSONValue {
        try await withTimeoutOrError(timeout, ChannelError.timeout(timeoutMessage)) {
            try await withTaskCancellationHandler {
                try await withCheckedThrowingContinuation { continuation in
                    self.internals.withLock {
                        $0.pendingReplies[ref] = continuation
                    }
                    // onCancel may have run before the continuation existed.
                    if Task.isCancelled {
                        self.failPending(ref: ref, error: ChannelError.timeout(timeoutMessage))
                        return
                    }
                    Task {
                        do {
                            try await self.liveSocket().send(
                                joinRef: joinRef, ref: ref, topic: self.topic,
                                event: event, payload: payload
                            )
                        } catch {
                            self.failPending(ref: ref, error: error)
                        }
                    }
                }
            } onCancel: {
                self.failPending(ref: ref, error: ChannelError.timeout(timeoutMessage))
            }
        }
    }

    private func failPending(ref: String, error: any Error) {
        let continuation = internals.withLock { $0.pendingReplies.removeValue(forKey: ref) }
        continuation?.resume(throwing: error)
    }

    // MARK: Event handlers

    /// Register a callback for a channel event. Returns an unsubscribe
    /// closure. Buffered pushes for the event are replayed immediately.
    @discardableResult
    public func on(
        _ event: String,
        _ callback: @escaping @Sendable (JSONValue) -> Void
    ) -> @Sendable () -> Void {
        let (id, replayed): (Int, [JSONValue]) = internals.withLock { state in
            state.handlerCounter += 1
            let id = state.handlerCounter
            state.eventHandlers[event, default: []].append((id: id, callback: callback))
            let pending = state.pendingPushes.removeValue(forKey: event) ?? []
            return (id, pending)
        }
        for payload in replayed {
            callback(payload)
        }
        return { [weak self] in
            guard let self else { return }
            self.internals.withLock {
                $0.eventHandlers[event]?.removeAll { $0.id == id }
            }
        }
    }

    // MARK: Inbound dispatch

    func onMessage(joinRef: String?, ref: String?, event: String, payload: JSONValue) {
        // Ignore messages from a stale join.
        let currentJoinRef = internals.withLock { $0.joinRef }
        if let joinRef, let currentJoinRef, joinRef != currentJoinRef { return }

        switch event {
        case "phx_reply":
            if let ref {
                let continuation = internals.withLock {
                    $0.pendingReplies.removeValue(forKey: ref)
                }
                continuation?.resume(returning: payload)
            }
        case "phx_close":
            internals.withLock { $0.state = .closed }
            trigger(event: "phx_close", payload: .object([:]))
        case "phx_error":
            internals.withLock { $0.state = .errored }
            trigger(event: "phx_error", payload: payload)
        default:
            let handlers = internals.withLock { state -> [(id: Int, callback: @Sendable (JSONValue) -> Void)] in
                let registered = state.eventHandlers[event] ?? []
                if registered.isEmpty {
                    var buffer = state.pendingPushes[event, default: []]
                    if buffer.count >= pendingPushCap {
                        buffer.removeFirst()
                    }
                    buffer.append(payload)
                    state.pendingPushes[event] = buffer
                }
                return registered
            }
            for handler in handlers {
                handler.callback(payload)
            }
        }
    }

    func handleSocketDisconnect() {
        let continuations = internals.withLock { state in
            let pending = state.pendingReplies
            state.pendingReplies = [:]
            // Retire the in-flight join: its reply can no longer arrive, and
            // a reconnect's rejoin() must start a fresh one.
            state.inFlightJoin = nil
            if state.state != .closed, state.state != .leaving {
                state.state = .errored
                state.joinRef = nil
            }
            return pending
        }
        for (_, continuation) in continuations {
            continuation.resume(throwing: ChannelError.notConnected("Socket disconnected"))
        }
    }

    private func trigger(event: String, payload: JSONValue) {
        let handlers = internals.withLock { $0.eventHandlers[event] ?? [] }
        for handler in handlers {
            handler.callback(payload)
        }
    }
}

// MARK: - Helpers

/// Race `operation` against a timeout; the losing side is cancelled.
func withTimeoutOrError<T: Sendable>(
    _ seconds: TimeInterval,
    _ timeoutError: @autoclosure @escaping @Sendable () -> any Error,
    operation: @escaping @Sendable () async throws -> T
) async throws -> T {
    try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask { try await operation() }
        group.addTask {
            try await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
            throw timeoutError()
        }
        guard let result = try await group.next() else {
            throw timeoutError()
        }
        group.cancelAll()
        return result
    }
}
