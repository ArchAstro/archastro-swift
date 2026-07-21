// Runtime: Phoenix Channels client for the generated Platform SDK.
// This file is hand-maintained, not generated. Port of the Python SDK's
// archastro/phx_channel package.
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

/// Manages a WebSocket connection to a Phoenix server: heartbeat, refs,
/// and per-topic channels.
public final class Socket: @unchecked Sendable {
    public static let defaultHeartbeatInterval: TimeInterval = 30
    public static let defaultTimeout: TimeInterval = 10

    private let urlString: String
    private let params: [String: String]
    private let heartbeatInterval: TimeInterval
    let timeout: TimeInterval
    // Reserved for a future reconnect implementation; the SDK currently
    // surfaces disconnects to callers instead of silently retrying.
    private let autoReconnect: Bool

    private struct State {
        var task: URLSessionWebSocketTask?
        var connected = false
        var closing = false
        var refCounter = 0
        var channels: [String: Channel] = [:]
        var pendingHeartbeatRef: String?
        var receiveTask: Task<Void, Never>?
        var heartbeatTask: Task<Void, Never>?
    }

    private let state = Locked(State())

    public init(
        url: String,
        params: [String: String] = [:],
        heartbeatInterval: TimeInterval = Socket.defaultHeartbeatInterval,
        timeout: TimeInterval = Socket.defaultTimeout,
        autoReconnect: Bool = true
    ) {
        self.urlString = url
        self.params = params
        self.heartbeatInterval = heartbeatInterval
        self.timeout = timeout
        self.autoReconnect = autoReconnect
    }

    public var isConnected: Bool {
        state.withLock { $0.connected }
    }

    // MARK: Connection lifecycle

    public func connect() async throws {
        guard var components = URLComponents(string: urlString) else {
            throw PlatformClientError.invalidURL(urlString)
        }
        var items = components.queryItems ?? []
        for (key, value) in params.sorted(by: { $0.key < $1.key }) {
            items.append(URLQueryItem(name: key, value: value))
        }
        items.append(URLQueryItem(name: "vsn", value: "2.0.0"))
        components.queryItems = items
        guard let url = components.url else {
            throw PlatformClientError.invalidURL(urlString)
        }

        let task = URLSession.shared.webSocketTask(with: url)
        state.withLock {
            $0.task = task
            $0.closing = false
        }
        task.resume()

        // The handshake completes lazily; a ping round-trip confirms it.
        try await withTimeoutOrError(timeout, ChannelError.timeout("Socket connect timed out")) {
            try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, any Error>) in
                task.sendPing { error in
                    if let error {
                        cont.resume(
                            throwing: PlatformClientError.connectionFailed(String(describing: error))
                        )
                    } else {
                        cont.resume(returning: ())
                    }
                }
            }
        }

        state.withLock { $0.connected = true }
        startReceiveLoop(task)
        startHeartbeatLoop()
    }

    public func disconnect() async {
        let (task, receive, heartbeat) = state.withLock { state in
            state.closing = true
            state.connected = false
            let result = (state.task, state.receiveTask, state.heartbeatTask)
            state.task = nil
            state.receiveTask = nil
            state.heartbeatTask = nil
            return result
        }
        receive?.cancel()
        heartbeat?.cancel()
        task?.cancel(with: .normalClosure, reason: nil)
    }

    // MARK: Channels

    /// Create (or return the existing) channel for a topic.
    public func channel(_ topic: String) -> Channel {
        state.withLock { state in
            if let existing = state.channels[topic] { return existing }
            let channel = Channel(socket: self, topic: topic)
            state.channels[topic] = channel
            return channel
        }
    }

    func removeChannel(_ topic: String) {
        state.withLock { _ = $0.channels.removeValue(forKey: topic) }
    }

    func makeRef() -> String {
        state.withLock { state in
            state.refCounter += 1
            return String(state.refCounter)
        }
    }

    // MARK: Sending

    func send(
        joinRef: String?,
        ref: String?,
        topic: String,
        event: String,
        payload: JSONValue
    ) async throws {
        let (task, isConnected) = state.withLock { ($0.task, $0.connected) }
        guard let task, isConnected else {
            throw ChannelError.notConnected("Socket is not connected")
        }
        let frame: JSONValue = .array([
            joinRef.map { .string($0) } ?? .null,
            ref.map { .string($0) } ?? .null,
            .string(topic),
            .string(event),
            payload,
        ])
        let data = try JSONCoding.encoder.encode(frame)
        try await task.send(.string(String(decoding: data, as: UTF8.self)))
    }

    // MARK: Receive loop

    private func startReceiveLoop(_ task: URLSessionWebSocketTask) {
        let loop = Task { [weak self] in
            while !Task.isCancelled {
                do {
                    let message = try await task.receive()
                    let text: String
                    switch message {
                    case .string(let string):
                        text = string
                    case .data(let data):
                        text = String(decoding: data, as: UTF8.self)
                    @unknown default:
                        continue
                    }
                    guard
                        let parsed = try? JSONCoding.decoder.decode(
                            JSONValue.self, from: Data(text.utf8)
                        ),
                        let frame = parsed.arrayValue,
                        frame.count == 5
                    else { continue }
                    self?.dispatch(
                        joinRef: frame[0].stringValue,
                        ref: frame[1].stringValue,
                        topic: frame[2].stringValue ?? "",
                        event: frame[3].stringValue ?? "",
                        payload: frame[4]
                    )
                } catch {
                    self?.handleDisconnect()
                    break
                }
            }
        }
        state.withLock { $0.receiveTask = loop }
    }

    private func handleDisconnect() {
        let openChannels = state.withLock { state -> [Channel] in
            state.connected = false
            return Array(state.channels.values)
        }
        for channel in openChannels {
            channel.handleSocketDisconnect()
        }
    }

    private func dispatch(
        joinRef: String?,
        ref: String?,
        topic: String,
        event: String,
        payload: JSONValue
    ) {
        let (isHeartbeatReply, channel) = state.withLock { state -> (Bool, Channel?) in
            let heartbeat = ref != nil && ref == state.pendingHeartbeatRef
            if heartbeat { state.pendingHeartbeatRef = nil }
            return (heartbeat, state.channels[topic])
        }
        if isHeartbeatReply { return }
        channel?.onMessage(joinRef: joinRef, ref: ref, event: event, payload: payload)
    }

    // MARK: Heartbeat

    private func startHeartbeatLoop() {
        let interval = heartbeatInterval
        let loop = Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: UInt64(interval * 1_000_000_000))
                guard let self, self.isConnected, !Task.isCancelled else { break }

                let stale = self.state.withLock { $0.pendingHeartbeatRef != nil }
                if stale {
                    // Previous heartbeat unacknowledged — connection is dead.
                    await self.disconnect()
                    break
                }
                let ref = self.makeRef()
                self.state.withLock { $0.pendingHeartbeatRef = ref }
                do {
                    try await self.send(
                        joinRef: nil, ref: ref, topic: "phoenix",
                        event: "heartbeat", payload: .object([:])
                    )
                } catch {
                    break
                }
            }
        }
        state.withLock { $0.heartbeatTask = loop }
    }
}

/// A single Phoenix Channel subscription on a topic.
public final class Channel: @unchecked Sendable {
    public enum State: String, Sendable {
        case closed, joining, joined, leaving, errored
    }

    private let socket: Socket
    public let topic: String

    private struct Internals {
        var state: Channel.State = .closed
        var joinRef: String?
        var pendingReplies: [String: CheckedContinuation<JSONValue, any Error>] = [:]
        var eventHandlers: [String: [(id: Int, callback: @Sendable (JSONValue) -> Void)]] = [:]
        var handlerCounter = 0
        // Inbound pushes that arrived before a handler was registered —
        // replayed to the first handler so autoPush-on-join can't be
        // missed. Bounded per event.
        var pendingPushes: [String: [JSONValue]] = [:]
    }

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

    // MARK: Join / leave

    /// Join the channel; returns the server's join response payload.
    /// Throws `ChannelError` if the join is rejected, times out, or the
    /// channel is already joined.
    public func join(
        _ payload: JSONValue? = nil,
        timeout: TimeInterval? = nil
    ) async throws -> JSONValue {
        let effectiveTimeout = timeout ?? socket.timeout
        let ref = socket.makeRef()

        try internals.withLock { state in
            if state.state == .joined {
                throw ChannelError.alreadyJoined(
                    "Channel \(topic) is already joined; call leave() first"
                )
            }
            state.state = .joining
            state.joinRef = ref
        }

        let envelope: JSONValue
        do {
            envelope = try await sendAndAwaitReply(
                joinRef: ref, ref: ref, event: "phx_join",
                payload: payload ?? .object([:]),
                timeout: effectiveTimeout,
                timeoutMessage: "Join timed out for \(topic)"
            )
        } catch {
            internals.withLock { $0.state = .errored }
            throw error
        }

        if envelope["status"]?.stringValue == "ok" {
            internals.withLock { $0.state = .joined }
            return envelope["response"] ?? .object([:])
        }
        internals.withLock { $0.state = .errored }
        throw ChannelError.joinRejected(envelope["response"] ?? .null)
    }

    /// Leave the channel (best-effort — a timeout is swallowed).
    public func leave(timeout: TimeInterval? = nil) async throws {
        if state == .closed { return }
        internals.withLock { $0.state = .leaving }

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
        socket.removeChannel(topic)
    }

    // MARK: Push / reply

    /// Push an event and await the reply envelope `{status, response}`.
    public func push(
        _ event: String,
        payload: some Encodable & Sendable,
        timeout: TimeInterval? = nil
    ) async throws -> ChannelReply {
        let payloadJSON = try JSONValue(encodable: payload)
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
                    Task {
                        do {
                            try await self.socket.send(
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
            state.state = .errored
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
