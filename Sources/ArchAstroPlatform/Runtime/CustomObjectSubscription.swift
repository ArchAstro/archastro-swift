// Copyright (c) 2026 ArchAstro Inc. Licensed under the MIT License.
// See LICENSE for details.

// Runtime: managed custom-object subscription (hand-maintained). Port of
// @archastro/sdk custom-object-subscriptions.ts over the generated
// ApiObjectChannel:
// - one socket per connection attempt (fresh token), exponential backoff
//   with jitter, terminal states for auth / not-found;
// - every join delivers the authoritative snapshot, then replays queued
//   patches; offline updates coalesce (deep merge) into the newest batch;
// - presence publishes at most every 50 ms and re-sends every 25 s.
// Swift additions: each batch carries an operation id (the server dedupes a
// replayed batch by id + digest, so a sent batch is never merged into), and
// replies are classified by reason code, not message text.

import Foundation

public struct CustomObjectSubscriptionOptions: Sendable {
    public var objectId: String
    /// Stable identity for this client. Generated when nil.
    public var connectionId: String?
    public var onSnapshot: @Sendable (CustomObjectSnapshot) -> Void
    public var onUpdate: @Sendable (CustomObjectUpdate) -> Void
    public var onPresence: (@Sendable (CustomObjectPresence) -> Void)?
    public var onPresenceLeave: (@Sendable (CustomObjectPresenceLeave) -> Void)?
    public var onStateChange: @Sendable (CustomObjectConnectionState) -> Void
    public var onError: @Sendable (CustomObjectSubscriptionError) -> Void
    /// Upper bound on the reconnect delay.
    public var maxReconnectDelay: TimeInterval = 10
    /// Maximum pending offline `update` calls.
    public var maxQueuedPatches = 100
    /// Presence is re-sent this often while live (below the server's TTL).
    var presenceRefreshInterval: Duration = .seconds(25)

    public init(
        objectId: String,
        connectionId: String? = nil,
        onSnapshot: @escaping @Sendable (CustomObjectSnapshot) -> Void,
        onUpdate: @escaping @Sendable (CustomObjectUpdate) -> Void,
        onPresence: (@Sendable (CustomObjectPresence) -> Void)? = nil,
        onPresenceLeave: (@Sendable (CustomObjectPresenceLeave) -> Void)? = nil,
        onStateChange: @escaping @Sendable (CustomObjectConnectionState) -> Void,
        onError: @escaping @Sendable (CustomObjectSubscriptionError) -> Void
    ) {
        self.objectId = objectId
        self.connectionId = connectionId
        self.onSnapshot = onSnapshot
        self.onUpdate = onUpdate
        self.onPresence = onPresence
        self.onPresenceLeave = onPresenceLeave
        self.onStateChange = onStateChange
        self.onError = onError
    }
}

/// Builds the socket for one connection attempt. It must not auto-reconnect:
/// the subscription owns reconnects so each attempt resolves a fresh token.
public typealias CustomObjectSocketFactory = @Sendable () async throws -> Socket

public final class CustomObjectSubscriptions: Sendable {
    private let makeSocket: CustomObjectSocketFactory

    public init(makeSocket: @escaping CustomObjectSocketFactory) {
        self.makeSocket = makeSocket
    }

    /// Start a subscription. It connects right away and reports through the
    /// option callbacks. Keep the returned subscription: releasing it drops
    /// its socket (call `close()` for an orderly leave).
    public func subscribe(_ options: CustomObjectSubscriptionOptions) -> CustomObjectSubscription {
        let subscription = CustomObjectSubscription(makeSocket: makeSocket, options: options)
        Task { await subscription.start() }
        return subscription
    }
}

public actor CustomObjectSubscription {
    static let presenceInterval: Duration = .milliseconds(50)

    private final class QueuedPatch {
        var fields: [String: JSONValue]
        let operationId: String
        /// Sent at least once. The server remembers id + digest, so a sent
        /// batch is frozen: later updates start a new batch.
        var sent: Bool
        /// One per `update` call coalesced into this batch.
        var waiters: [SubscriptionPromise]

        init(fields: [String: JSONValue], operationId: String, sent: Bool, waiter: SubscriptionPromise) {
            self.fields = fields
            self.operationId = operationId
            self.sent = sent
            self.waiters = [waiter]
        }
    }

    public private(set) var state: CustomObjectConnectionState = .connecting
    private let makeSocket: CustomObjectSocketFactory
    let options: CustomObjectSubscriptionOptions
    private var connectionId: String
    private var socket: Socket?
    private(set) var channel: ApiObjectChannel?
    private var transportUnsubscribers: [@Sendable () -> Void] = []
    private var reconnectTask: Task<Void, Never>?
    /// Why the subscription ended (close or a terminal error). Work that
    /// resumes after an await settles with this instead of re-queueing.
    private(set) var endedWith: CustomObjectSubscriptionError?
    private var reconnectAttempt = 0
    private(set) var generation = 0
    private var connecting = false
    private var queued: [QueuedPatch] = []
    var lastPresence: CustomObjectPresenceUpdate?
    var pendingPresence: CustomObjectPresenceUpdate?
    var presenceWaiters: [SubscriptionPromise] = []
    var presenceFlushTask: Task<Void, Never>?
    var presenceHeartbeatTask: Task<Void, Never>?
    var presenceSending = false
    var lastPresenceSentAt: ContinuousClock.Instant?

    init(makeSocket: @escaping CustomObjectSocketFactory, options: CustomObjectSubscriptionOptions) {
        self.makeSocket = makeSocket
        self.options = options
        self.connectionId = options.connectionId ?? UUID().uuidString.lowercased()
    }

    func start() async {
        guard state != .closed else { return }
        options.onStateChange(.connecting)
        await connect(reconnecting: false)
    }

    // MARK: Public operations

    /// Deep-merge `fields` into the object. Live: sent now. Otherwise queued
    /// and sent after the next join; resolves once the server accepts it.
    public func update(_ fields: [String: JSONValue]) async throws {
        if isTerminal {
            throw CustomObjectSubscriptionError.network("Cannot update a \(state.rawValue) custom-object subscription")
        }
        guard state == .live, let channel else {
            return try await enqueue(fields)
        }
        let operationId = Self.newOperationId()
        let generation = generation
        var failure: CustomObjectSubscriptionError
        do {
            let reply = try await channel.updateFields(payload: .init(fields: fields, operationId: operationId))
            if reply.status == "ok" { return }
            failure = .fromReply(reply.response)
        } catch {
            failure = .classify(error)
        }
        if let endedWith { throw endedWith }
        guard case .network = failure else {
            options.onError(failure)
            if failure.isTerminal { terminate(failure) }
            throw failure
        }
        // The send may have landed: keep its id so a replay dedupes. It was
        // sent before anything queued since, so it replays first.
        let waiter = SubscriptionPromise()
        if let rejection = queueRejection() {
            waiter.reject(rejection)
        } else {
            queued.insert(QueuedPatch(fields: fields, operationId: operationId, sent: true, waiter: waiter), at: 0)
        }
        handleTransportLoss(failure, generation: generation)
        try await waiter.value()
    }

    /// Publish this client's cursor, selection and activity (throttled to
    /// 50 ms; re-sent every 25 s while live).
    public func updatePresence(_ presence: CustomObjectPresenceUpdate) async throws {
        guard state == .live, channel != nil else {
            throw CustomObjectSubscriptionError.network("Presence is unavailable while the subscription is not live")
        }
        lastPresence = presence
        startPresenceHeartbeat()
        try await enqueuePresence(presence)
    }

    /// Persist the live object now.
    public func save() async throws {
        guard state == .live, let channel else {
            throw CustomObjectSubscriptionError.network("Cannot save while the subscription is not live")
        }
        let generation = generation
        let classified: CustomObjectSubscriptionError
        do {
            let reply = try await channel.save()
            if reply.status == "ok" { return }
            classified = .fromReply(reply.response)
        } catch {
            classified = .classify(error)
        }
        if let endedWith { throw endedWith }
        if classified.isTerminal {
            options.onError(classified)
            terminate(classified)
        } else if case .network = classified {
            handleTransportLoss(classified, generation: generation)
        } else {
            options.onError(classified)
        }
        throw classified
    }

    public func close() {
        guard state != .closed else { return }
        let closed = CustomObjectSubscriptionError.network("Custom-object subscription was closed")
        endedWith = endedWith ?? closed
        generation += 1
        reconnectTask?.cancel()
        reconnectTask = nil
        transition(.closed)
        rejectQueued(closed)
        disposeTransport(graceful: true)
    }

    // MARK: Connection

    private func connect(reconnecting: Bool) async {
        guard !connecting, !isTerminal else { return }
        connecting = true
        defer { connecting = false }
        generation += 1
        let generation = generation
        if reconnecting { transition(.reconnecting) }

        do {
            let socket = try await makeSocket()
            self.socket = socket
            try await socket.connect()
            guard generation == self.generation, !isTerminal else {
                await socket.disconnect()
                return
            }
            transportUnsubscribers.append(socket.onEvent { [weak self] event in
                switch event {
                case .open: return
                case .close(let code, _):
                    Task { await self?.handleTransportLoss(.network("WebSocket closed with code \(code)"), generation: generation) }
                case .error(let message):
                    Task { await self?.handleTransportLoss(.network(message), generation: generation) }
                }
            })

            let channel = try await ApiObjectChannel.joinById(
                socket: socket, objectId: options.objectId,
                connectionId: connectionId, partialUpdates: true
            )
            guard generation == self.generation, !isTerminal else {
                try? await channel.leave()
                await socket.disconnect()
                return
            }
            self.channel = channel
            installChannelListeners(channel, generation: generation)

            // Every join, including a rejoin, returns the authoritative
            // snapshot. Deliver it before replaying queued patches.
            let (snapshot, presence) = try parseSnapshot(channel.joinResponse)
            if let id = snapshot.connectionId { connectionId = id }
            options.onSnapshot(snapshot)
            for entry in presence { options.onPresence?(entry) }
            try await replayQueuedPatches(generation: generation)
            guard generation == self.generation, !isTerminal else { return }
            reconnectAttempt = 0
            transition(.live)
            if let lastPresence {
                // Through the publisher, whose error handling only reconnects
                // on transport loss: a refused presence must not undo the join.
                lastPresenceSentAt = nil
                pendingPresence = lastPresence
                schedulePresenceFlush()
                startPresenceHeartbeat()
            }
        } catch {
            guard generation == self.generation, !isTerminal else { return }
            let classified = CustomObjectSubscriptionError.classify(error)
            options.onError(classified)
            if classified.isTerminal {
                terminate(classified)
            } else {
                scheduleReconnect()
            }
        }
    }

    private func installChannelListeners(_ channel: ApiObjectChannel, generation: Int) {
        let hop: @Sendable (@escaping @Sendable (isolated CustomObjectSubscription) -> Void) -> Void = { [weak self] body in
            Task { [weak self] in
                guard let self else { return }
                await self.run(generation: generation, body)
            }
        }
        transportUnsubscribers += [
            channel.onObjectUpdated { payload in
                hop { $0.options.onUpdate(CustomObjectUpdate(
                    id: payload.id, fields: payload.fields,
                    operationId: payload.operationId, partial: payload.partial
                )) }
            },
            channel.onObjectDeleted { _ in
                hop {
                    let error = CustomObjectSubscriptionError.notFound(code: "custom_object_deleted")
                    $0.options.onError(error)
                    $0.terminate(error)
                }
            },
            channel.onAccessRevoked { _ in
                hop {
                    let error = CustomObjectSubscriptionError.authorization(code: "custom_object_access_revoked")
                    $0.options.onError(error)
                    $0.terminate(error)
                }
            },
            channel.onPresenceUpdated { payload in
                let raw = JSONValue.object(payload.presence)
                hop { me in
                    if let presence = CustomObjectWire.presence(raw) {
                        me.options.onPresence?(presence)
                    } else {
                        me.reportMalformed("Malformed presence_updated payload")
                    }
                }
            },
            channel.onPresenceLeft { payload in
                hop { $0.options.onPresenceLeave?(CustomObjectPresenceLeave(connectionId: payload.connectionId)) }
            },
            // The server-side channel crashed while the socket stayed up.
            channel.channel.on("phx_error") { _ in
                hop { $0.handleTransportLoss(.network("channel error"), generation: generation) }
            },
        ]
    }

    /// Run a channel callback on the actor unless its connection is gone.
    private func run(generation: Int, _ body: (isolated CustomObjectSubscription) -> Void) {
        guard generation == self.generation, !isTerminal else { return }
        body(self)
    }

    private func parseSnapshot(_ payload: JSONValue?) throws -> (CustomObjectSnapshot, [CustomObjectPresence]) {
        guard let envelope = CustomObjectWire.envelope(payload) else {
            throw CustomObjectSubscriptionError.validation(code: "malformed_snapshot", details: nil)
        }
        var presence: [CustomObjectPresence] = []
        for item in payload?["presence"]?.arrayValue ?? [] {
            if let parsed = CustomObjectWire.presence(item) {
                presence.append(parsed)
            } else {
                reportMalformed("Malformed initial presence payload")
            }
        }
        let snapshot = CustomObjectSnapshot(
            id: envelope.id,
            fields: envelope.fields,
            readonly: payload?["readonly"]?.boolValue,
            connectionId: (payload?["connection_id"] ?? payload?["connectionId"])?.stringValue
        )
        return (snapshot, presence)
    }

    private func reportMalformed(_ message: String) {
        options.onError(.validation(code: "malformed_realtime_payload", details: .string(message)))
    }

    // MARK: Offline queue

    private func queueRejection() -> CustomObjectSubscriptionError? {
        let pending = queued.reduce(0) { $0 + $1.waiters.count }
        return pending >= options.maxQueuedPatches
            ? .validation(code: "offline_queue_full", details: nil)
            : nil
    }

    private func enqueue(_ fields: [String: JSONValue]) async throws {
        let waiter = SubscriptionPromise()
        if let rejection = queueRejection() {
            waiter.reject(rejection)
        } else if let last = queued.last, !last.sent {
            last.fields = CustomObjectWire.deepMerge(last.fields, fields)
            last.waiters.append(waiter)
        } else {
            queued.append(QueuedPatch(fields: fields, operationId: Self.newOperationId(), sent: false, waiter: waiter))
        }
        try await waiter.value()
    }

    private func replayQueuedPatches(generation: Int) async throws {
        while generation == self.generation, let patch = queued.first, let channel {
            patch.sent = true
            // Updates arriving while this reply is pending form a new batch.
            let reply = try await channel.updateFields(
                payload: .init(fields: patch.fields, operationId: patch.operationId)
            )
            guard queued.first === patch else { return }
            queued.removeFirst()
            if reply.status == "ok" {
                for waiter in patch.waiters { waiter.resolve() }
                continue
            }
            let classified = CustomObjectSubscriptionError.fromReply(reply.response)
            if classified.isTerminal {
                queued.insert(patch, at: 0)
                throw classified
            }
            if case .network = classified {
                // Temporary (the object's server restarted): keep the batch,
                // unchanged under its id, and reconnect.
                queued.insert(patch, at: 0)
                throw classified
            }
            // A refused batch (readonly, validation) cannot succeed on retry.
            options.onError(classified)
            for waiter in patch.waiters { waiter.reject(classified) }
        }
    }

    private func rejectQueued(_ error: CustomObjectSubscriptionError) {
        let batches = queued
        queued = []
        for batch in batches {
            for waiter in batch.waiters { waiter.reject(error) }
        }
    }

    // MARK: Lifecycle

    func handleTransportLoss(_ error: CustomObjectSubscriptionError, generation: Int) {
        guard generation == self.generation, !isTerminal else { return }
        options.onError(error)
        if error.isTerminal {
            terminate(error)
        } else {
            scheduleReconnect()
        }
    }

    private func scheduleReconnect() {
        guard !isTerminal, reconnectTask == nil else { return }
        // Supersede an in-flight connect(): it must not go live on the
        // transport torn down here.
        generation += 1
        disposeTransport(graceful: false)
        transition(.reconnecting)
        let base = min(0.25 * pow(2, Double(reconnectAttempt)), options.maxReconnectDelay)
        reconnectAttempt += 1
        let delay = base + Double.random(in: 0..<max(0.001, base * 0.2))
        reconnectTask = Task { [weak self] in
            try? await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
            guard !Task.isCancelled else { return }
            await self?.fireReconnect()
        }
    }

    private func fireReconnect() async {
        reconnectTask = nil
        if connecting {
            // A superseded attempt is still unwinding; try again later.
            scheduleReconnect()
            return
        }
        await connect(reconnecting: true)
    }

    private func disposeTransport(graceful: Bool) {
        stopPresenceHeartbeat()
        stopPresencePublisher()
        for unsubscribe in transportUnsubscribers { unsubscribe() }
        transportUnsubscribers = []
        let channel = channel
        let socket = socket
        self.channel = nil
        self.socket = nil
        Task {
            if graceful { try? await channel?.leave() }
            await socket?.disconnect()
        }
    }

    private func transition(_ next: CustomObjectConnectionState) {
        guard state != next else { return }
        state = next
        options.onStateChange(next)
    }

    private var isTerminal: Bool {
        state == .closed || state == .unauthorized
    }

    func terminate(_ error: CustomObjectSubscriptionError) {
        endedWith = endedWith ?? error
        generation += 1
        reconnectTask?.cancel()
        reconnectTask = nil
        transition(error.terminalState)
        rejectQueued(error)
        disposeTransport(graceful: false)
    }

    static func newOperationId() -> String {
        "op_" + UUID().uuidString.lowercased()
    }
}
