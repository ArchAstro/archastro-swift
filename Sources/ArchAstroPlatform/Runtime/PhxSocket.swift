// Copyright (c) 2026 ArchAstro Inc. Licensed under the MIT License.
// See LICENSE for details.

// Runtime: Phoenix socket for the generated Platform SDK (hand-maintained).
// Connection lifecycle mirrors @archastro/sdk phx_channel/socket.ts:
// open/close/error events, heartbeat, auto-reconnect on the Phoenix backoff
// schedule, and rejoin of joined or errored channels after a reconnect.
//
// Wire protocol: JSON arrays [join_ref, ref, topic, event, payload], vsn 2.0.0.

import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

/// Socket lifecycle events, as @archastro/sdk `SocketEvent`.
public enum SocketEvent: Sendable, Equatable {
    case open
    /// The transport closed. `code` is the WebSocket close code (1006 when
    /// the connection dropped without a close frame).
    case close(code: Int, reason: String)
    case error(String)
}

/// Connect params resolved on every (re)connect, so a reconnect carries a
/// fresh access token instead of the one the socket was created with.
public typealias SocketParamsProvider = @Sendable () async throws -> [String: String]

/// A text WebSocket. `URLSessionPhxTransport` is the default; tests inject
/// an in-memory one.
public protocol PhxTransport: Sendable {
    /// Resolves once the handshake completes.
    func open() async throws
    func send(_ text: String) async throws
    /// Next text frame. Throws `PhxTransportClosed` when the peer closes.
    func receive() async throws -> String
    func close(code: Int, reason: String)
}

/// Thrown by `PhxTransport.receive()` when the connection ends.
public struct PhxTransportClosed: Error, Sendable, Equatable {
    public let code: Int
    public let reason: String

    public init(code: Int, reason: String) {
        self.code = code
        self.reason = reason
    }
}

public typealias PhxTransportFactory = @Sendable (URL) -> any PhxTransport

/// `URLSessionWebSocketTask` transport.
public final class URLSessionPhxTransport: PhxTransport, @unchecked Sendable {
    private let task: URLSessionWebSocketTask

    public init(url: URL, session: URLSession = .shared) {
        task = session.webSocketTask(with: url)
    }

    public func open() async throws {
        task.resume()
        // The handshake completes lazily; a ping round-trip confirms it.
        // Cancelling (the connect timeout) cancels the task, which fails the ping.
        try await withTaskCancellationHandler {
            try await ping()
        } onCancel: { [task] in
            task.cancel(with: .goingAway, reason: nil)
        }
    }

    private func ping() async throws {
        try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, any Error>) in
            task.sendPing { error in
                if let error {
                    cont.resume(throwing: PlatformClientError.connectionFailed(String(describing: error)))
                } else {
                    cont.resume(returning: ())
                }
            }
        }
    }

    public func send(_ text: String) async throws {
        try await task.send(.string(text))
    }

    public func receive() async throws -> String {
        do {
            switch try await task.receive() {
            case .string(let text): return text
            case .data(let data): return String(decoding: data, as: UTF8.self)
            @unknown default: return ""
            }
        } catch {
            let code = task.closeCode == .invalid ? 1006 : task.closeCode.rawValue
            let reason = task.closeReason.map { String(decoding: $0, as: UTF8.self) } ?? ""
            throw PhxTransportClosed(code: code, reason: reason)
        }
    }

    public func close(code: Int, reason: String) {
        let closeCode = URLSessionWebSocketTask.CloseCode(rawValue: code) ?? .normalClosure
        task.cancel(with: closeCode, reason: Data(reason.utf8))
    }
}

/// Manages a WebSocket connection to a Phoenix server: heartbeat, refs,
/// per-topic channels, and reconnect.
public final class Socket: @unchecked Sendable {
    public static let defaultHeartbeatInterval: TimeInterval = 30
    public static let defaultTimeout: TimeInterval = 10
    /// Reconnect backoff (seconds), matching the Phoenix JS client.
    public static let defaultReconnectBackoff: [TimeInterval] = [
        0.01, 0.05, 0.1, 0.15, 0.2, 0.25, 0.5, 1, 2,
    ]

    private let urlString: String
    private let params: [String: String]
    private let paramsProvider: SocketParamsProvider?
    private let heartbeatInterval: TimeInterval
    let timeout: TimeInterval
    private let autoReconnect: Bool
    private let reconnectBackoff: [TimeInterval]
    private let makeTransport: PhxTransportFactory

    private struct State {
        var transport: (any PhxTransport)?
        /// Bumped per transport so a stale receive loop or heartbeat from an
        /// earlier connection cannot tear down the current one.
        var generation = 0
        var connected = false
        var closing = false
        var refCounter = 0
        var reconnectAttempt = 0
        var channels: [String: Channel] = [:]
        var pendingHeartbeatRef: String?
        var receiveTask: Task<Void, Never>?
        var heartbeatTask: Task<Void, Never>?
        var reconnectTask: Task<Void, Never>?
        var listenerCounter = 0
        var listeners: [(id: Int, callback: @Sendable (SocketEvent) -> Void)] = []
    }

    private let state = Locked(State())

    public init(
        url: String,
        params: [String: String] = [:],
        paramsProvider: SocketParamsProvider? = nil,
        heartbeatInterval: TimeInterval = Socket.defaultHeartbeatInterval,
        timeout: TimeInterval = Socket.defaultTimeout,
        autoReconnect: Bool = true,
        reconnectBackoff: [TimeInterval] = Socket.defaultReconnectBackoff,
        transport: @escaping PhxTransportFactory = { URLSessionPhxTransport(url: $0) }
    ) {
        self.urlString = url
        self.params = params
        self.paramsProvider = paramsProvider
        self.heartbeatInterval = heartbeatInterval
        self.timeout = timeout
        self.autoReconnect = autoReconnect
        self.reconnectBackoff = reconnectBackoff.isEmpty ? Socket.defaultReconnectBackoff : reconnectBackoff
        self.makeTransport = transport
    }

    public var isConnected: Bool {
        state.withLock { $0.connected }
    }

    // MARK: Events

    /// Observe open/close/error. Returns an unsubscribe closure.
    @discardableResult
    public func onEvent(_ listener: @escaping @Sendable (SocketEvent) -> Void) -> @Sendable () -> Void {
        let id = state.withLock { state -> Int in
            state.listenerCounter += 1
            state.listeners.append((id: state.listenerCounter, callback: listener))
            return state.listenerCounter
        }
        return { [weak self] in
            self?.state.withLock { $0.listeners.removeAll { $0.id == id } }
        }
    }

    private func emit(_ event: SocketEvent) {
        let listeners = state.withLock { $0.listeners }
        for listener in listeners { listener.callback(event) }
    }

    // MARK: Connection lifecycle

    /// Connect once. Throws if this first attempt fails; after a connection
    /// has opened, drops are retried on the backoff schedule when
    /// `autoReconnect` is on.
    public func connect() async throws {
        if isConnected { return }
        let previous = state.withLock { state -> Task<Void, Never>? in
            state.closing = false
            state.reconnectAttempt = 0
            defer { state.reconnectTask = nil }
            return state.reconnectTask
        }
        previous?.cancel()
        try await open()
    }

    public func disconnect() async {
        let (transport, tasks, channels) = state.withLock { state in
            state.closing = true
            state.generation += 1
            let wasOpen = state.connected ? state.transport : nil
            state.connected = false
            state.transport = nil
            let tasks = [state.receiveTask, state.heartbeatTask, state.reconnectTask]
            state.receiveTask = nil
            state.heartbeatTask = nil
            state.reconnectTask = nil
            state.pendingHeartbeatRef = nil
            return (wasOpen, tasks, Array(state.channels.values))
        }
        for task in tasks { task?.cancel() }
        guard let transport else { return }
        transport.close(code: 1000, reason: "client disconnect")
        for channel in channels { channel.handleSocketDisconnect() }
        emit(.close(code: 1000, reason: "client disconnect"))
    }

    private func open() async throws {
        var opening: (any PhxTransport)?
        do {
            var merged = params
            if let paramsProvider {
                for (key, value) in try await paramsProvider() { merged[key] = value }
            }
            let transport = makeTransport(try connectURL(merged))
            opening = transport
            try await withTimeoutOrError(timeout, ChannelError.timeout("Socket connect timed out")) {
                try await transport.open()
            }
        } catch {
            opening?.close(code: 1000, reason: "connect failed")
            emit(.error(String(describing: error)))
            throw error
        }
        guard let transport = opening else { return }

        enum Accept { case accepted(Int, [Channel]), redundant, closing }
        let outcome = state.withLock { state -> Accept in
            guard !state.closing else { return .closing }
            // Another open() won the race (user connect() vs backoff retry).
            guard !state.connected else { return .redundant }
            state.generation += 1
            state.transport = transport
            state.connected = true
            state.reconnectAttempt = 0
            state.pendingHeartbeatRef = nil
            return .accepted(state.generation, Array(state.channels.values))
        }
        switch outcome {
        case .closing:
            transport.close(code: 1000, reason: "client disconnect")
            throw ChannelError.notConnected("Socket was disconnected while connecting")
        case .redundant:
            transport.close(code: 1000, reason: "already connected")
        case .accepted(let generation, let channels):
            startReceiveLoop(transport, generation: generation)
            startHeartbeatLoop(generation: generation)
            emit(.open)
            for channel in channels where channel.state == .joined || channel.state == .errored {
                Task { await channel.rejoin() }
            }
        }
    }

    private func connectURL(_ params: [String: String]) throws -> URL {
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
        return url
    }

    /// The current transport ended (peer close, network loss, heartbeat
    /// timeout). Fail in-flight replies, report the close, and reconnect.
    private func transportLost(generation: Int, code: Int, reason: String) {
        let lost = state.withLock { state -> (any PhxTransport, [Channel], [Task<Void, Never>?])? in
            guard state.generation == generation, state.connected, let transport = state.transport else {
                return nil
            }
            state.connected = false
            state.transport = nil
            state.pendingHeartbeatRef = nil
            let tasks = [state.heartbeatTask]
            state.heartbeatTask = nil
            return (transport, Array(state.channels.values), tasks)
        }
        guard let (transport, channels, tasks) = lost else { return }
        for task in tasks { task?.cancel() }
        // Close our side normally; report the peer's code (1006 is reserved
        // and must never be sent in a close frame).
        transport.close(code: 1000, reason: "")
        for channel in channels { channel.handleSocketDisconnect() }
        emit(.close(code: code, reason: reason))
        scheduleReconnect()
    }

    private func scheduleReconnect() {
        guard autoReconnect else { return }
        state.withLock { state in
            guard !state.closing else { return }
            let delay = reconnectBackoff[min(state.reconnectAttempt, reconnectBackoff.count - 1)]
            state.reconnectAttempt += 1
            state.reconnectTask = Task { [weak self] in
                try? await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
                guard let self, self.shouldRetry() else { return }
                do {
                    try await self.open()
                } catch {
                    if self.shouldRetry() { self.scheduleReconnect() }
                }
            }
        }
    }

    /// A backoff retry proceeds only if nobody connected, disconnected or
    /// superseded it (connect() cancels the pending retry) meanwhile.
    private func shouldRetry() -> Bool {
        !Task.isCancelled && state.withLock { !$0.closing && !$0.connected }
    }

    deinit {
        let (transport, tasks) = state.withLock { state in
            (state.transport, [state.receiveTask, state.heartbeatTask, state.reconnectTask])
        }
        for task in tasks { task?.cancel() }
        transport?.close(code: 1000, reason: "socket released")
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
        let transport = state.withLock { $0.connected ? $0.transport : nil }
        guard let transport else {
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
        try await transport.send(String(decoding: data, as: UTF8.self))
    }

    // MARK: Receive loop

    private func startReceiveLoop(_ transport: any PhxTransport, generation: Int) {
        let loop = Task { [weak self] in
            while !Task.isCancelled {
                do {
                    let text = try await transport.receive()
                    guard
                        let parsed = try? JSONCoding.decoder.decode(JSONValue.self, from: Data(text.utf8)),
                        let frame = parsed.arrayValue,
                        frame.count == 5
                    else { continue }
                    self?.dispatch(
                        generation: generation,
                        joinRef: frame[0].stringValue,
                        ref: frame[1].stringValue,
                        topic: frame[2].stringValue ?? "",
                        event: frame[3].stringValue ?? "",
                        payload: frame[4]
                    )
                } catch let closed as PhxTransportClosed {
                    self?.transportLost(generation: generation, code: closed.code, reason: closed.reason)
                    break
                } catch {
                    self?.transportLost(generation: generation, code: 1006, reason: String(describing: error))
                    break
                }
            }
        }
        state.withLock { state in
            if state.generation == generation { state.receiveTask = loop }
        }
    }

    private func dispatch(
        generation: Int,
        joinRef: String?,
        ref: String?,
        topic: String,
        event: String,
        payload: JSONValue
    ) {
        let (isHeartbeatReply, channel) = state.withLock { state -> (Bool, Channel?) in
            // Frames from a superseded connection are dropped.
            guard state.generation == generation else { return (true, nil) }
            let heartbeat = ref != nil && ref == state.pendingHeartbeatRef
            if heartbeat { state.pendingHeartbeatRef = nil }
            return (heartbeat, state.channels[topic])
        }
        if isHeartbeatReply { return }
        channel?.onMessage(joinRef: joinRef, ref: ref, event: event, payload: payload)
    }

    // MARK: Heartbeat

    private func startHeartbeatLoop(generation: Int) {
        let interval = heartbeatInterval
        let loop = Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: UInt64(interval * 1_000_000_000))
                guard let self, !Task.isCancelled else { break }
                let beat = self.state.withLock { state -> (stale: Bool, ref: String?)? in
                    guard state.generation == generation, state.connected else { return nil }
                    if state.pendingHeartbeatRef != nil { return (true, nil) }
                    state.refCounter += 1
                    let ref = String(state.refCounter)
                    state.pendingHeartbeatRef = ref
                    return (false, ref)
                }
                guard let beat else { break }
                if beat.stale {
                    // Previous heartbeat unacknowledged: the connection is dead.
                    self.transportLost(generation: generation, code: 1000, reason: "heartbeat timeout")
                    break
                }
                do {
                    try await self.send(
                        joinRef: nil, ref: beat.ref, topic: "phoenix",
                        event: "heartbeat", payload: .object([:])
                    )
                } catch {
                    self.transportLost(generation: generation, code: 1000, reason: "heartbeat send failed")
                    break
                }
            }
        }
        state.withLock { state in
            if state.generation == generation { state.heartbeatTask = loop }
        }
    }
}
