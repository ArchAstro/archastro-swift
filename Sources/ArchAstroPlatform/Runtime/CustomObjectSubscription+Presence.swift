// Copyright (c) 2026 ArchAstro Inc. Licensed under the MIT License.
// See LICENSE for details.

// Runtime: presence publishing for CustomObjectSubscription (hand-maintained).
// As @archastro/sdk: the newest update wins, at most one send per 50 ms,
// the last presence is re-sent every 25 s while live (below the server's
// TTL), and a `rate_limited` reply is not an error.

import Foundation

extension CustomObjectSubscription {

    func startPresenceHeartbeat() {
        stopPresenceHeartbeat()
        guard lastPresence != nil, state == .live else { return }
        let interval = options.presenceRefreshInterval
        presenceHeartbeatTask = Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(for: interval)
                guard !Task.isCancelled else { return }
                guard let self else { return }
                await self.resendPresence()
            }
        }
    }

    private func resendPresence() async {
        guard channel != nil, state == .live, let lastPresence else { return }
        try? await enqueuePresence(lastPresence)
    }

    func stopPresenceHeartbeat() {
        presenceHeartbeatTask?.cancel()
        presenceHeartbeatTask = nil
    }

    func enqueuePresence(_ presence: CustomObjectPresenceUpdate) async throws {
        let waiter = SubscriptionPromise()
        pendingPresence = presence
        presenceWaiters.append(waiter)
        schedulePresenceFlush()
        try await waiter.value()
    }

    func schedulePresenceFlush() {
        guard presenceFlushTask == nil, !presenceSending, pendingPresence != nil else { return }
        let wait: Duration = lastPresenceSentAt.map { Self.presenceInterval - ($0.duration(to: .now)) } ?? .zero
        presenceFlushTask = Task { [weak self] in
            if wait > .zero { try? await Task.sleep(for: wait) }
            guard !Task.isCancelled else { return }
            await self?.flushPresence()
        }
    }

    private func flushPresence() async {
        presenceFlushTask = nil
        guard !presenceSending, let presence = pendingPresence else { return }
        guard state == .live, let channel else {
            rejectPendingPresence(.network("Presence is unavailable while the subscription is not live"))
            return
        }
        let waiters = presenceWaiters
        presenceWaiters = []
        pendingPresence = nil
        presenceSending = true
        lastPresenceSentAt = .now
        let generation = generation
        do {
            try await Self.sendPresence(channel, presence)
            for waiter in waiters { waiter.resolve() }
        } catch {
            let classified = endedWith ?? CustomObjectSubscriptionError.classify(error)
            if endedWith != nil {
                // Closed meanwhile: settle, nothing to recover.
            } else if classified.isTerminal {
                options.onError(classified)
                terminate(classified)
            } else if case .network = classified {
                handleTransportLoss(classified, generation: generation)
            } else {
                options.onError(classified)
            }
            for waiter in waiters { waiter.reject(classified) }
        }
        presenceSending = false
        schedulePresenceFlush()
    }

    func stopPresencePublisher() {
        presenceFlushTask?.cancel()
        presenceFlushTask = nil
        lastPresenceSentAt = nil
        rejectPendingPresence(.network("Presence is unavailable while the subscription is not live"))
    }

    private func rejectPendingPresence(_ error: CustomObjectSubscriptionError) {
        pendingPresence = nil
        let waiters = presenceWaiters
        presenceWaiters = []
        for waiter in waiters { waiter.reject(error) }
    }

    /// A `rate_limited` presence reply is not an error: the next update wins.
    static func sendPresence(_ channel: ApiObjectChannel, _ presence: CustomObjectPresenceUpdate) async throws {
        let reply = try await channel.presenceUpdate(payload: .init(presence: presence.wire.objectValue ?? [:]))
        guard reply.status != "ok" else { return }
        if CustomObjectSubscriptionError.reasonCode(reply.response) == "rate_limited" { return }
        throw CustomObjectSubscriptionError.fromReply(reply.response)
    }
}
