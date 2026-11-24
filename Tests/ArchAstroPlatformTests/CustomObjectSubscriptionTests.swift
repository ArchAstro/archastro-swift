// Copyright (c) 2026 ArchAstro Inc. Licensed under the MIT License.
// See LICENSE for details.

// Managed custom-object subscription, ported from @archastro/sdk
// __tests__/custom-object-subscriptions.test.ts, against an in-memory
// api:object channel (deep merge, operation-id dedupe, presence).

import Foundation
import Testing
@testable import ArchAstroPlatform

/// In-memory `api:object:*` semantics on top of `FakePhoenixServer`.
final class FakeObjectServer: @unchecked Sendable {
    let phoenix = FakePhoenixServer()
    let fields = Locked<[String: JSONValue]>(["title": "Plan", "meta": ["v": 1]])
    let joinError = Locked<String?>(nil)
    let updateError = Locked<String?>(nil)
    let presenceError = Locked<String?>(nil)
    let holdUpdates = Locked(false)
    let operations = Locked<[(id: String, fields: [String: JSONValue])]>([])
    let presenceSent = Locked<[JSONValue]>([])
    let joinCount = Locked(0)
    let initialPresence = Locked<[JSONValue]>([])
    private let seen = Locked<[String: String]>([:])

    init() {
        phoenix.custom.withLock { $0 = { [weak self] joinRef, ref, topic, event, payload in
            self?.handle(joinRef, ref, topic, event, payload)
        } }
    }

    private func handle(_ joinRef: String?, _ ref: String?, _ topic: String, _ event: String, _ payload: JSONValue) -> [String]? {
        guard topic.hasPrefix("api:object:") else { return nil }
        let id = String(topic.dropFirst("api:object:".count))
        func ok(_ response: JSONValue) -> [String] { [FakePhoenixServer.reply(joinRef, ref, topic, response)] }
        // Like api_object_channel.ex: named reasons are plain strings; the
        // catch-all replies `inspect(atom)`, e.g. ":rate_limited".
        func error(_ reason: String) -> [String] {
            let named: Set = ["unauthorized", "not_found", "deleted", "readonly", "validation", "object_unavailable", "invalid_operation_id"]
            let wire = named.contains(reason) ? reason : ":" + reason
            return [FakePhoenixServer.frame(joinRef, ref, topic, "phx_reply", ["status": "error", "response": ["reason": .string(wire)]])]
        }
        switch event {
        case "phx_join":
            joinCount.withLock { $0 += 1 }
            if let reason = joinError.withLock({ $0 }) { return error(reason) }
            return ok([
                "id": .string(id), "fields": .object(fields.withLock { $0 }), "readonly": false,
                "connection_id": payload["connection_id"] ?? "conn_server",
                "presence": .array(initialPresence.withLock { $0 }),
            ])
        case "update_fields":
            if holdUpdates.withLock({ $0 }) { return [] }
            if let reason = updateError.withLock({ $0 }) { return error(reason) }
            let patch = payload["fields"]?.objectValue ?? [:]
            let opId = payload["operation_id"]?.stringValue ?? "op_server"
            let digest = String(describing: JSONValue.object(patch))
            let previous = seen.withLock { seen -> String? in
                defer { if seen[opId] == nil { seen[opId] = digest } }
                return seen[opId]
            }
            if let previous {
                return previous == digest ? ok(["fields": .object(fields.withLock { $0 }), "operation_id": .string(opId)]) : error("operation_id_conflict")
            }
            operations.withLock { $0.append((opId, patch)) }
            let merged = fields.withLock { current -> [String: JSONValue] in
                current = CustomObjectWire.deepMerge(current, patch)
                return current
            }
            return ok(["fields": .object(merged), "operation_id": .string(opId)])
        case "save":
            return ok([:])
        case "presence_update":
            presenceSent.withLock { $0.append(payload["presence"] ?? .null) }
            if let reason = presenceError.withLock({ $0 }) { return error(reason) }
            return ok([:])
        case "phx_leave":
            return ok([:])
        default:
            return nil
        }
    }

    func push(_ event: String, _ payload: JSONValue, objectId: String = "obj_1") {
        phoenix.latest?.push(topic: "api:object:\(objectId)", event: event, payload: payload)
    }
}

/// Everything a subscription reported.
final class Recorder: @unchecked Sendable {
    let snapshots = Locked<[CustomObjectSnapshot]>([])
    let updates = Locked<[CustomObjectUpdate]>([])
    let presence = Locked<[CustomObjectPresence]>([])
    let leaves = Locked<[String]>([])
    let states = Locked<[CustomObjectConnectionState]>([])
    let errors = Locked<[CustomObjectSubscriptionError]>([])

    func options(_ objectId: String = "obj_1") -> CustomObjectSubscriptionOptions {
        var options = CustomObjectSubscriptionOptions(
            objectId: objectId,
            connectionId: "conn_me",
            onSnapshot: { snapshot in self.snapshots.withLock { $0.append(snapshot) } },
            onUpdate: { update in self.updates.withLock { $0.append(update) } },
            onPresence: { presence in self.presence.withLock { $0.append(presence) } },
            onPresenceLeave: { leave in self.leaves.withLock { $0.append(leave.connectionId) } },
            onStateChange: { state in self.states.withLock { $0.append(state) } },
            onError: { error in self.errors.withLock { $0.append(error) } }
        )
        options.maxReconnectDelay = 0.02
        return options
    }

    var state: CustomObjectConnectionState? { states.withLock { $0.last } }
}

@Suite struct CustomObjectSubscriptionTests {
    private func subscriptions(_ server: FakeObjectServer) -> CustomObjectSubscriptions {
        CustomObjectSubscriptions {
            Socket(
                url: "ws://platform.test/socket/api/websocket",
                paramsProvider: { ["token": "tok"] },
                timeout: 1, autoReconnect: false, transport: server.phoenix.factory
            )
        }
    }

    private func live(_ server: FakeObjectServer, _ recorder: Recorder, _ options: CustomObjectSubscriptionOptions? = nil) async throws -> CustomObjectSubscription {
        let subscription = subscriptions(server).subscribe(options ?? recorder.options())
        try await eventually("live") { recorder.state == .live }
        return subscription
    }

    @Test func delivers_the_join_snapshot_and_remote_events() async throws {
        let server = FakeObjectServer()
        server.initialPresence.withLock { $0 = [[
            "user_id": "usr_2", "connection_id": "conn_2", "display_name": "Ada",
            "selected_element_ids": ["el_1"], "state": "active", "cursor": ["x": 1, "y": 2.5],
        ]] }
        let recorder = Recorder()
        let subscription = try await live(server, recorder)

        #expect(recorder.states.withLock { $0 } == [.connecting, .live])
        let snapshot = try #require(recorder.snapshots.withLock { $0.first })
        #expect(snapshot.id == "obj_1")
        #expect(snapshot.fields["title"] == "Plan")
        #expect(snapshot.readonly == false)
        #expect(snapshot.connectionId == "conn_me")
        let ada = try #require(recorder.presence.withLock { $0.first })
        #expect(ada.cursor == CustomObjectCursor(x: 1, y: 2.5))
        #expect(ada.color == CustomObjectWire.presenceColor("usr_2"))

        server.push("object_updated", ["id": "obj_1", "fields": ["title": "Next"], "operation_id": "op_x", "partial": true])
        server.push("presence_left", ["connection_id": "conn_2"])
        try await eventually("events") { recorder.updates.withLock { $0.count } == 1 && recorder.leaves.withLock { $0.count } == 1 }
        #expect(recorder.updates.withLock { $0[0] } == CustomObjectUpdate(id: "obj_1", fields: ["title": "Next"], operationId: "op_x", partial: true))
        await subscription.close()
    }

    @Test func live_updates_carry_operation_ids() async throws {
        let server = FakeObjectServer()
        let recorder = Recorder()
        let subscription = try await live(server, recorder)
        try await subscription.update(["meta": ["w": 2]])
        let op = try #require(server.operations.withLock { $0.first })
        #expect(op.id.hasPrefix("op_"))
        #expect(server.fields.withLock { $0["meta"] } == ["v": 1, "w": 2])
        await subscription.close()
    }

    @Test func recovers_the_snapshot_before_replaying_compacted_offline_patches() async throws {
        let server = FakeObjectServer()
        let recorder = Recorder()
        let subscription = try await live(server, recorder)

        server.joinError.withLock { $0 = "object_unavailable" }
        server.phoenix.latest?.peerClose(code: 1006)
        try await eventually("reconnecting") { recorder.state == .reconnecting }
        async let first: Void = subscription.update(["meta": ["a": 1]])
        async let second: Void = subscription.update(["meta": ["b": 2], "title": "Offline"])
        try await Task.sleep(nanoseconds: 30_000_000)
        server.joinError.withLock { $0 = nil }
        _ = try await (first, second)

        // One compacted batch, applied after the fresh snapshot.
        let ops = server.operations.withLock { $0 }
        #expect(ops.count == 1)
        #expect(ops.first?.fields == ["meta": ["a": 1, "b": 2], "title": "Offline"])
        #expect(recorder.snapshots.withLock { $0.count } == 2)
        #expect(recorder.state == .live)
        await subscription.close()
    }

    @Test func a_caller_operation_id_is_sent_live_and_echoed() async throws {
        let server = FakeObjectServer()
        let recorder = Recorder()
        let subscription = try await live(server, recorder)
        try await subscription.update(["title": "Mine"], operationId: "op_caller_1")
        #expect(server.operations.withLock { $0.map(\.id) } == ["op_caller_1"])
        server.push("object_updated", ["id": "obj_1", "fields": ["title": "Mine"], "operation_id": "op_caller_1", "partial": true])
        try await eventually("echo") { recorder.updates.withLock { $0.count } == 1 }
        #expect(recorder.updates.withLock { $0.first?.operationId } == "op_caller_1")
        await subscription.close()
    }

    @Test func caller_owned_offline_patches_replay_one_by_one_under_their_ids() async throws {
        let server = FakeObjectServer()
        let recorder = Recorder()
        let subscription = try await live(server, recorder)
        server.joinError.withLock { $0 = "object_unavailable" }
        server.phoenix.latest?.peerClose(code: 1006)
        try await eventually("reconnecting") { recorder.state == .reconnecting }
        let first = Task { try await subscription.update(["meta": ["a": 1]], operationId: "op_a") }
        try await Task.sleep(nanoseconds: 5_000_000)
        let second = Task { try await subscription.update(["meta": ["b": 2]], operationId: "op_b") }
        try await Task.sleep(nanoseconds: 5_000_000)
        let third = Task { try await subscription.update(["title": "Anonymous"]) }
        try await Task.sleep(nanoseconds: 5_000_000)
        let fourth = Task { try await subscription.update(["meta": ["c": 3]]) }
        try await Task.sleep(nanoseconds: 20_000_000)
        server.joinError.withLock { $0 = nil }
        _ = try await (first.value, second.value, third.value, fourth.value)

        // Owned patches keep their own ids, in order; anonymous ones still compact.
        let ops = server.operations.withLock { $0 }
        #expect(ops.count == 3)
        #expect(ops.map(\.id).prefix(2) == ["op_a", "op_b"])
        #expect(ops.first?.fields == ["meta": ["a": 1]])
        #expect(ops.last?.fields == ["title": "Anonymous", "meta": ["c": 3]])
        #expect(server.fields.withLock { $0 } == ["title": "Anonymous", "meta": ["v": 1, "a": 1, "b": 2, "c": 3]])
        await subscription.close()
    }

    @Test func an_update_queued_while_a_replay_is_in_flight_is_not_lost() async throws {
        let server = FakeObjectServer()
        let recorder = Recorder()
        let subscription = try await live(server, recorder)
        server.joinError.withLock { $0 = "object_unavailable" }
        server.phoenix.latest?.peerClose(code: 1006)
        try await eventually("reconnecting") { recorder.state == .reconnecting }
        let first = Task { try await subscription.update(["a": 1]) }
        try await Task.sleep(nanoseconds: 10_000_000)
        server.holdUpdates.withLock { $0 = true }
        server.joinError.withLock { $0 = nil }
        // The replay of {a} is now waiting for its reply.
        try await eventually("rejoined") { server.joinCount.withLock { $0 } >= 2 && recorder.snapshots.withLock { $0.count } == 2 }
        let second = Task { try await subscription.update(["b": 2]) }
        try await Task.sleep(nanoseconds: 20_000_000)
        server.holdUpdates.withLock { $0 = false }
        // The held reply never comes; drop the connection to force a replay.
        server.phoenix.latest?.peerClose(code: 1006)
        try await first.value
        try await second.value
        let applied = server.fields.withLock { $0 }
        #expect(applied["a"] == 1)
        #expect(applied["b"] == 2)
        // {a} was replayed under its original id, never merged with {b}.
        #expect(server.operations.withLock { $0.map(\.fields) } == [["a": 1], ["b": 2]])
        await subscription.close()
    }

    @Test func bounds_pending_offline_updates_even_when_they_compact() async throws {
        let server = FakeObjectServer()
        server.joinError.withLock { $0 = "object_unavailable" }
        let recorder = Recorder()
        var options = recorder.options()
        options.maxQueuedPatches = 2
        let subscription = subscriptions(server).subscribe(options)
        let one = Task { try await subscription.update(["a": 1]) }
        let two = Task { try await subscription.update(["a": 2]) }
        try await Task.sleep(nanoseconds: 20_000_000)
        await #expect(throws: CustomObjectSubscriptionError.validation(code: "offline_queue_full", details: nil)) {
            try await subscription.update(["a": 3])
        }
        await subscription.close()
        await #expect(throws: CustomObjectSubscriptionError.self) { try await one.value }
        await #expect(throws: CustomObjectSubscriptionError.self) { try await two.value }
    }

    @Test func unauthorized_joins_are_terminal_and_do_not_reconnect() async throws {
        let server = FakeObjectServer()
        server.joinError.withLock { $0 = "unauthorized" }
        let recorder = Recorder()
        let subscription = subscriptions(server).subscribe(recorder.options())
        try await eventually("unauthorized") { recorder.state == .unauthorized }
        try await Task.sleep(nanoseconds: 80_000_000)
        #expect(server.joinCount.withLock { $0 } == 1)
        #expect(recorder.errors.withLock { $0 } == [.authorization(code: "unauthorized")])
        await #expect(throws: CustomObjectSubscriptionError.self) { try await subscription.update(["a": 1]) }
    }

    @Test func a_deleted_object_closes_and_revoked_access_is_unauthorized() async throws {
        let server = FakeObjectServer()
        let deleted = Recorder()
        let first = try await live(server, deleted)
        server.push("object_deleted", ["id": "obj_1", "reason": "deleted"])
        try await eventually("closed") { deleted.state == .closed }
        #expect(deleted.errors.withLock { $0 } == [.notFound(code: "custom_object_deleted")])

        let revoked = Recorder()
        let second = try await live(server, revoked)
        server.push("access_revoked", ["reason": "unauthorized"])
        try await eventually("unauthorized") { revoked.state == .unauthorized }
        _ = (first, second)
    }

    @Test func a_refused_update_is_reported_and_the_subscription_stays_live() async throws {
        let server = FakeObjectServer()
        let recorder = Recorder()
        let subscription = try await live(server, recorder)
        server.updateError.withLock { $0 = "readonly" }
        await #expect(throws: CustomObjectSubscriptionError.rejected(code: "readonly")) {
            try await subscription.update(["a": 1])
        }
        #expect(recorder.state == .live)
        await subscription.close()
    }

    @Test func malformed_snapshots_and_payloads_are_reported() async throws {
        let server = FakeObjectServer()
        let recorder = Recorder()
        let subscription = try await live(server, recorder)
        server.push("presence_updated", ["presence": ["user_id": "usr_2"]])
        try await eventually("malformed") { !recorder.errors.withLock { $0 }.isEmpty }
        #expect(recorder.errors.withLock { $0.first } == .validation(code: "malformed_realtime_payload", details: "Malformed presence_updated payload"))
        await subscription.close()
    }

    @Test func close_suppresses_reconnect_and_listeners() async throws {
        let server = FakeObjectServer()
        let recorder = Recorder()
        let subscription = try await live(server, recorder)
        let transport = server.phoenix.latest
        await subscription.close()
        transport?.push(topic: "api:object:obj_1", event: "object_updated", payload: ["id": "obj_1", "fields": [:], "operation_id": "o", "partial": true])
        transport?.peerClose(code: 1006)
        try await Task.sleep(nanoseconds: 80_000_000)
        #expect(recorder.state == .closed)
        #expect(recorder.updates.withLock { $0 }.isEmpty)
        #expect(server.joinCount.withLock { $0 } == 1)
    }

    @Test func repeated_reconnects_do_not_duplicate_listeners() async throws {
        let server = FakeObjectServer()
        let recorder = Recorder()
        let subscription = try await live(server, recorder)
        for round in 2...4 {
            server.phoenix.latest?.peerClose(code: 1006)
            try await eventually("rejoin \(round)") { recorder.snapshots.withLock { $0.count } == round && recorder.state == .live }
        }
        server.push("object_updated", ["id": "obj_1", "fields": ["n": 1], "operation_id": "o", "partial": true])
        try await Task.sleep(nanoseconds: 40_000_000)
        #expect(recorder.updates.withLock { $0.count } == 1)
        await subscription.close()
    }

    @Test func presence_coalesces_onto_a_50ms_interval_and_refreshes() async throws {
        let server = FakeObjectServer()
        let recorder = Recorder()
        var options = recorder.options()
        options.presenceRefreshInterval = .milliseconds(120)
        let subscription = try await live(server, recorder, options)
        let first = Task { try await subscription.updatePresence(.init(cursor: .init(x: 1, y: 1), selectedElementIds: [], activity: .active)) }
        try await first.value
        // Four updates inside one 50 ms window, enqueued in order.
        var burst: [Task<Void, any Error>] = []
        for n in 2...5 {
            burst.append(Task { try await subscription.updatePresence(.init(cursor: .init(x: Double(n), y: 1), selectedElementIds: [], activity: .active)) })
            try await Task.sleep(nanoseconds: 2_000_000)
        }
        for task in burst { try await task.value }
        let sent = server.presenceSent.withLock { $0 }
        // First send, then the burst coalesced (a stalled CI runner may split it once).
        #expect((2...3).contains(sent.count))
        #expect(sent.last?["cursor"]?["x"]?.doubleValue == 5)
        #expect(sent.last?["state"] == "active")
        try await eventually("refresh") { server.presenceSent.withLock { $0.count } >= 3 }
        await subscription.close()
    }

    @Test func rate_limited_presence_keeps_the_transport_and_forbidden_terminates() async throws {
        let server = FakeObjectServer()
        let recorder = Recorder()
        let subscription = try await live(server, recorder)
        server.presenceError.withLock { $0 = "rate_limited" }
        try await subscription.updatePresence(.init(cursor: nil, selectedElementIds: [], activity: .idle))
        #expect(recorder.state == .live)
        server.presenceError.withLock { $0 = "unauthorized" }
        try await Task.sleep(nanoseconds: 60_000_000)
        await #expect(throws: CustomObjectSubscriptionError.authorization(code: "unauthorized")) {
            try await subscription.updatePresence(.init(cursor: nil, selectedElementIds: [], activity: .idle))
        }
        #expect(recorder.state == .unauthorized)
    }

    @Test func presence_reconnects_on_a_network_failure() async throws {
        let server = FakeObjectServer()
        let recorder = Recorder()
        let subscription = try await live(server, recorder)
        server.phoenix.custom.withLock { previous in
            let inner = previous
            previous = { joinRef, ref, topic, event, payload in
                if event == "presence_update" { return [] } // never answered
                return inner?(joinRef, ref, topic, event, payload)
            }
        }
        let pending = Task { try await subscription.updatePresence(.init(cursor: nil, selectedElementIds: [], activity: .active)) }
        try await Task.sleep(nanoseconds: 20_000_000)
        server.phoenix.latest?.peerClose(code: 1006)
        await #expect(throws: CustomObjectSubscriptionError.self) { try await pending.value }
        try await eventually("back live") { recorder.snapshots.withLock { $0.count } == 2 && recorder.state == .live }
        await subscription.close()
    }

    @Test func handshake_403_is_unauthorized() {
        #expect(CustomObjectSubscriptionError.classify(PhxTransportRejected(status: 403)) == .authorization(code: "forbidden"))
        #expect(CustomObjectSubscriptionError.classify(PhxTransportRejected(status: 502)).isTerminal == false)
        #expect(CustomObjectSubscriptionError.fromReply(["reason": "deleted"]) == .notFound(code: "deleted"))
        #expect(CustomObjectSubscriptionError.fromReply(["reason": "object_unavailable"]).isTerminal == false)
    }

    @Test func deep_merge_merges_objects_and_replaces_everything_else() {
        let merged = CustomObjectWire.deepMerge(
            ["a": ["x": 1, "y": 2], "list": [1, 2], "s": "old"],
            ["a": ["y": 3], "list": [3], "s": .null]
        )
        #expect(merged == ["a": ["x": 1, "y": 3], "list": [3], "s": .null])
    }

    // MARK: Review regressions (PR #13)

    @Test func close_during_an_in_flight_update_settles_it() async throws {
        let server = FakeObjectServer()
        let recorder = Recorder()
        let subscription = try await live(server, recorder)
        server.holdUpdates.withLock { $0 = true }
        let pending = Task { try await subscription.update(["a": 1]) }
        try await Task.sleep(nanoseconds: 20_000_000)
        await subscription.close()
        await #expect(throws: CustomObjectSubscriptionError.network("Custom-object subscription was closed")) {
            try await pending.value
        }
    }

    @Test func room_unavailable_during_replay_keeps_the_batch_and_reconnects() async throws {
        let server = FakeObjectServer()
        let recorder = Recorder()
        let subscription = try await live(server, recorder)
        server.joinError.withLock { $0 = "object_unavailable" }
        server.phoenix.latest?.peerClose(code: 1006)
        try await eventually("reconnecting") { recorder.state == .reconnecting }
        let pending = Task { try await subscription.update(["keep": true]) }
        try await Task.sleep(nanoseconds: 10_000_000)
        server.updateError.withLock { $0 = "room_unavailable" }
        server.joinError.withLock { $0 = nil }
        try await eventually("replay refused once") { server.joinCount.withLock { $0 } >= 3 }
        server.updateError.withLock { $0 = nil }
        try await pending.value
        #expect(server.fields.withLock { $0["keep"] } == true)
        await subscription.close()
    }

    @Test func a_server_channel_crash_rejoins() async throws {
        let server = FakeObjectServer()
        let recorder = Recorder()
        let subscription = try await live(server, recorder)
        server.push("phx_error", [:])
        try await eventually("rejoined") { recorder.snapshots.withLock { $0.count } == 2 && recorder.state == .live }
        await subscription.close()
    }

    @Test func a_refused_presence_after_rejoin_keeps_the_connection() async throws {
        let server = FakeObjectServer()
        let recorder = Recorder()
        let subscription = try await live(server, recorder)
        try await subscription.updatePresence(.init(cursor: nil, selectedElementIds: ["a"], activity: .active))
        server.presenceError.withLock { $0 = "invalid_presence" }
        server.phoenix.latest?.peerClose(code: 1006)
        try await eventually("rejoined") { recorder.snapshots.withLock { $0.count } == 2 && recorder.state == .live }
        try await Task.sleep(nanoseconds: 100_000_000)
        #expect(recorder.state == .live)
        #expect(server.joinCount.withLock { $0 } == 2)
        #expect(recorder.errors.withLock { $0 }.contains(.rejected(code: "invalid_presence")))
        await subscription.close()
    }

    @Test func reason_codes_from_inspect_are_normalized() {
        #expect(CustomObjectSubscriptionError.reasonCode(["reason": ":rate_limited"]) == "rate_limited")
        #expect(CustomObjectSubscriptionError.fromReply(["reason": ":room_unavailable"]) == .network("room_unavailable"))
    }
}
