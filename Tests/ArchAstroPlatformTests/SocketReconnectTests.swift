// Copyright (c) 2026 ArchAstro Inc. Licensed under the MIT License.
// See LICENSE for details.

// Socket lifecycle: events, reconnect with fresh params, channel rejoin,
// heartbeat timeout, and disconnect. Runs against an in-memory Phoenix
// server behind `PhxTransport`; no network.

import Foundation
import Testing
@testable import ArchAstroPlatform

/// One in-memory connection. The socket reads from `inbox`; `send` hands
/// frames to the fake server, which may answer.
final class FakeTransport: PhxTransport, @unchecked Sendable {
    let url: URL
    private let server: FakePhoenixServer
    private let inbox: AsyncThrowingStream<String, any Error>
    private let feed: AsyncThrowingStream<String, any Error>.Continuation
    private let reader = Locked<AsyncThrowingStream<String, any Error>.AsyncIterator?>(nil)
    let closedWith = Locked<PhxTransportClosed?>(nil)
    /// Close codes this side asked to send.
    let localCloses = Locked<[Int]>([])

    init(url: URL, server: FakePhoenixServer) {
        self.url = url
        self.server = server
        (inbox, feed) = AsyncThrowingStream.makeStream(of: String.self)
        reader.withLock { $0 = inbox.makeAsyncIterator() }
    }

    func open() async throws {
        if server.refuseOpen.withLock({ $0 }) {
            throw PlatformClientError.connectionFailed("refused")
        }
    }

    func send(_ text: String) async throws {
        guard closedWith.withLock({ $0 }) == nil else { throw PhxTransportClosed(code: 1006, reason: "") }
        for reply in server.handle(text) { feed.yield(reply) }
    }

    func receive() async throws -> String {
        var iterator = reader.withLock { $0 }!
        let next = try await iterator.next()
        reader.withLock { $0 = iterator }
        guard let next else {
            throw closedWith.withLock { $0 } ?? PhxTransportClosed(code: 1006, reason: "")
        }
        return next
    }

    func close(code: Int, reason: String) {
        localCloses.withLock { $0.append(code) }
        peerClose(code: code, reason: reason)
    }

    /// The server (or network) ends the connection.
    func peerClose(code: Int, reason: String = "") {
        let first = closedWith.withLock { state -> Bool in
            guard state == nil else { return false }
            state = PhxTransportClosed(code: code, reason: reason)
            return true
        }
        if first { feed.finish() }
    }

    func push(topic: String, event: String, payload: JSONValue) {
        feed.yield(FakePhoenixServer.frame(nil, nil, topic, event, payload))
    }
}

/// Answers joins with a numbered snapshot, heartbeats unless muted, and
/// `echo` pushes; `hang` never gets a reply.
final class FakePhoenixServer: @unchecked Sendable {
    let refuseOpen = Locked(false)
    let muteHeartbeats = Locked(false)
    let transports = Locked<[FakeTransport]>([])
    let joins = Locked<[(topic: String, payload: JSONValue)]>([])

    var factory: PhxTransportFactory {
        { [self] url in
            let transport = FakeTransport(url: url, server: self)
            transports.withLock { $0.append(transport) }
            return transport
        }
    }

    var latest: FakeTransport? { transports.withLock { $0.last } }

    func handle(_ text: String) -> [String] {
        guard
            let frame = try? JSONCoding.decoder.decode(JSONValue.self, from: Data(text.utf8)).arrayValue,
            frame.count == 5
        else { return [] }
        let joinRef = frame[0].stringValue
        let ref = frame[1].stringValue
        let topic = frame[2].stringValue ?? ""
        let event = frame[3].stringValue ?? ""
        let payload = frame[4]
        switch event {
        case "phx_join":
            let count = joins.withLock { joins -> Int in
                joins.append((topic, payload))
                return joins.count
            }
            return [Self.reply(joinRef, ref, topic, ["snapshot": .int(count), "params": payload])]
        case "heartbeat":
            return muteHeartbeats.withLock { $0 } ? [] : [Self.reply(nil, ref, topic, [:])]
        case "echo", "phx_leave":
            return [Self.reply(joinRef, ref, topic, payload)]
        default:
            return []
        }
    }

    static func reply(_ joinRef: String?, _ ref: String?, _ topic: String, _ response: JSONValue) -> String {
        frame(joinRef, ref, topic, "phx_reply", ["status": "ok", "response": response])
    }

    static func frame(_ joinRef: String?, _ ref: String?, _ topic: String, _ event: String, _ payload: JSONValue) -> String {
        let value: JSONValue = .array([
            joinRef.map { .string($0) } ?? .null, ref.map { .string($0) } ?? .null,
            .string(topic), .string(event), payload,
        ])
        return String(decoding: try! JSONCoding.encoder.encode(value), as: UTF8.self)
    }
}

/// Poll until `condition` holds or fail after `timeout`.
func eventually(
    _ what: String,
    timeout: TimeInterval = 2,
    _ condition: @Sendable () -> Bool
) async throws {
    let deadline = Date().addingTimeInterval(timeout)
    while !condition() {
        if Date() > deadline {
            Issue.record("timed out waiting for \(what)")
            return
        }
        try await Task.sleep(nanoseconds: 5_000_000)
    }
}

@Suite struct SocketReconnectTests {
    private func makeSocket(
        _ server: FakePhoenixServer,
        autoReconnect: Bool = true,
        heartbeat: TimeInterval = 30,
        tokens: Locked<Int> = Locked(0)
    ) -> Socket {
        Socket(
            url: "ws://platform.test/socket/api/websocket",
            params: ["api_key": "pk_test"],
            paramsProvider: {
                let n = tokens.withLock { count -> Int in count += 1; return count }
                return ["token": "tok_\(n)"]
            },
            heartbeatInterval: heartbeat,
            timeout: 1,
            autoReconnect: autoReconnect,
            reconnectBackoff: [0.01],
            transport: server.factory
        )
    }

    private func recorder(_ socket: Socket) -> Locked<[SocketEvent]> {
        let events = Locked<[SocketEvent]>([])
        socket.onEvent { event in events.withLock { $0.append(event) } }
        return events
    }

    @Test func reconnects_with_fresh_params_and_rejoins_with_the_join_payload() async throws {
        let server = FakePhoenixServer()
        let socket = makeSocket(server)
        let events = recorder(socket)
        try await socket.connect()

        let channel = socket.channel("api:object:obj_1")
        let rejoins = Locked<[JSONValue]>([])
        channel.on(Channel.rejoinedEvent) { response in rejoins.withLock { $0.append(response) } }
        let first = try await channel.join(["partial_updates": true])
        #expect(first["snapshot"] == 1)

        server.latest?.peerClose(code: 1006)
        try await eventually("rejoin") { rejoins.withLock { $0.count } == 1 }

        #expect(events.withLock { $0 } == [.open, .close(code: 1006, reason: ""), .open])
        #expect(channel.state == .joined)
        #expect(rejoins.withLock { $0.first?["snapshot"] } == 2)
        let joins = server.joins.withLock { $0 }
        #expect(joins.map(\.payload) == [["partial_updates": true], ["partial_updates": true]])
        let urls = server.transports.withLock { $0.map(\.url.query) }
        #expect(urls == ["api_key=pk_test&token=tok_1&vsn=2.0.0", "api_key=pk_test&token=tok_2&vsn=2.0.0"])

        // Pushes work on the new connection.
        let reply = try await channel.push("echo", payload: ["n": 1] as JSONValue)
        #expect(reply.status == "ok")
        await socket.disconnect()
    }

    @Test func an_in_flight_push_fails_when_the_connection_drops() async throws {
        let server = FakePhoenixServer()
        let socket = makeSocket(server, autoReconnect: false)
        try await socket.connect()
        let channel = socket.channel("room:1")
        _ = try await channel.join()

        let pending = Task { try await channel.push("hang", payload: JSONValue.object([:])) }
        try await Task.sleep(nanoseconds: 20_000_000)
        server.latest?.peerClose(code: 1011, reason: "server error")
        await #expect(throws: ChannelError.self) { _ = try await pending.value }
        #expect(channel.state == .errored)
    }

    @Test func without_auto_reconnect_a_drop_is_final() async throws {
        let server = FakePhoenixServer()
        let socket = makeSocket(server, autoReconnect: false)
        let events = recorder(socket)
        try await socket.connect()
        server.latest?.peerClose(code: 4000, reason: "gone")
        try await eventually("close") { events.withLock { $0.count } == 2 }
        try await Task.sleep(nanoseconds: 50_000_000)
        #expect(events.withLock { $0 } == [.open, .close(code: 4000, reason: "gone")])
        #expect(server.transports.withLock { $0.count } == 1)
        #expect(!socket.isConnected)
    }

    @Test func disconnect_reports_a_normal_close_and_never_reconnects() async throws {
        let server = FakePhoenixServer()
        let socket = makeSocket(server)
        let events = recorder(socket)
        try await socket.connect()
        await socket.disconnect()
        try await Task.sleep(nanoseconds: 50_000_000)
        #expect(events.withLock { $0 } == [.open, .close(code: 1000, reason: "client disconnect")])
        #expect(server.transports.withLock { $0.count } == 1)
    }

    @Test func a_missed_heartbeat_drops_the_connection_and_reconnects() async throws {
        let server = FakePhoenixServer()
        server.muteHeartbeats.withLock { $0 = true }
        let socket = makeSocket(server, heartbeat: 0.02)
        let events = recorder(socket)
        try await socket.connect()
        try await eventually("reconnect") { events.withLock { $0.count } >= 3 }
        #expect(events.withLock { Array($0.prefix(3)) } == [.open, .close(code: 1000, reason: "heartbeat timeout"), .open])
        await socket.disconnect()
    }

    @Test func failed_reconnects_retry_until_the_server_is_back() async throws {
        let server = FakePhoenixServer()
        let socket = makeSocket(server)
        let events = recorder(socket)
        try await socket.connect()
        server.refuseOpen.withLock { $0 = true }
        server.latest?.peerClose(code: 1006)
        try await eventually("errors") {
            events.withLock { $0.filter { if case .error = $0 { true } else { false } }.count } >= 2
        }
        server.refuseOpen.withLock { $0 = false }
        try await eventually("reopen") { socket.isConnected }
        #expect(events.withLock { $0.last } == .open)
        await socket.disconnect()
    }

    @Test func a_channel_left_before_the_drop_is_not_rejoined() async throws {
        let server = FakePhoenixServer()
        let socket = makeSocket(server)
        try await socket.connect()
        let channel = socket.channel("room:left")
        _ = try await channel.join()
        try await channel.leave()
        server.latest?.peerClose(code: 1006)
        try await eventually("reconnect") { socket.isConnected && server.transports.withLock { $0.count } == 2 }
        try await Task.sleep(nanoseconds: 50_000_000)
        #expect(server.joins.withLock { $0.count } == 1)
        #expect(channel.state == .closed)
        await socket.disconnect()
    }

    @Test func the_first_connect_throws_instead_of_retrying() async throws {
        let server = FakePhoenixServer()
        server.refuseOpen.withLock { $0 = true }
        let socket = makeSocket(server)
        await #expect(throws: PlatformClientError.self) { try await socket.connect() }
        try await Task.sleep(nanoseconds: 50_000_000)
        #expect(server.transports.withLock { $0.count } == 1)
    }

    // MARK: Review regressions (PR #11)

    @Test func connect_while_connected_opens_no_second_transport() async throws {
        let server = FakePhoenixServer()
        let socket = makeSocket(server)
        try await socket.connect()
        try await socket.connect()
        #expect(server.transports.withLock { $0.count } == 1)
        await socket.disconnect()
    }

    @Test func a_user_connect_during_backoff_leaves_one_live_transport() async throws {
        let server = FakePhoenixServer()
        let socket = Socket(
            url: "ws://platform.test/socket", heartbeatInterval: 30, timeout: 1,
            reconnectBackoff: [0.1], transport: server.factory
        )
        try await socket.connect()
        server.latest?.peerClose(code: 1006)
        try await eventually("drop") { !socket.isConnected }
        try await socket.connect()
        try await Task.sleep(nanoseconds: 250_000_000)
        let live = server.transports.withLock { $0.filter { $0.closedWith.withLock { $0 } == nil } }
        #expect(live.count == 1)
        #expect(socket.isConnected)
        await socket.disconnect()
    }

    @Test func concurrent_joins_share_one_phx_join() async throws {
        let server = FakePhoenixServer()
        let socket = makeSocket(server)
        try await socket.connect()
        let channel = socket.channel("room:shared")
        async let first = channel.join(["n": 1])
        async let second = channel.join(["n": 1])
        let (a, b) = try await (first, second)
        #expect(a == b)
        #expect(server.joins.withLock { $0.count } == 1)
        #expect(channel.state == .joined)
        await socket.disconnect()
    }

    @Test func a_manual_rejoin_after_leave_is_not_reported_as_a_reconnect_rejoin() async throws {
        let server = FakePhoenixServer()
        let socket = makeSocket(server)
        try await socket.connect()
        let channel = socket.channel("room:manual")
        let rejoins = Locked(0)
        channel.on(Channel.rejoinedEvent) { _ in rejoins.withLock { $0 += 1 } }
        _ = try await channel.join()
        try await channel.leave()
        _ = try await socket.channel("room:manual").join()
        try await Task.sleep(nanoseconds: 30_000_000)
        #expect(rejoins.withLock { $0 } == 0)
        await socket.disconnect()
    }

    @Test func releasing_the_socket_closes_its_transport_and_stops_reconnecting() async throws {
        let server = FakePhoenixServer()
        var socket: Socket? = makeSocket(server)
        try await socket?.connect()
        let channel = socket?.channel("room:orphan")
        _ = try await channel?.join()
        socket = nil
        try await eventually("transport closed") { server.latest?.closedWith.withLock { $0 } != nil }
        try await Task.sleep(nanoseconds: 50_000_000)
        #expect(server.transports.withLock { $0.count } == 1)
        _ = channel
    }

    @Test func the_peer_close_code_is_reported_but_never_sent() async throws {
        let server = FakePhoenixServer()
        let socket = makeSocket(server, autoReconnect: false)
        let events = recorder(socket)
        try await socket.connect()
        let transport = server.latest
        transport?.peerClose(code: 1006)
        try await eventually("close") { events.withLock { $0.count } == 2 }
        #expect(events.withLock { $0.last } == .close(code: 1006, reason: ""))
        #expect(transport?.localCloses.withLock { $0 } == [1000])
    }
}
