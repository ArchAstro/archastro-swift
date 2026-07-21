// Test support: client for the channel-harness service's control API.
// This file is hand-maintained, not generated — the Swift counterpart to
// archastro.phx_channel.HarnessServiceClient (Python) and the TS client in
// @archastro/channel-harness.
//
// The harness service exposes two surfaces:
//   1. A WebSocket endpoint carrying the real Phoenix channel protocol.
//   2. An HTTP control endpoint for scenario registration, observations,
//      and reset. The same listener serves SSE routes for stream tests.
//
// There is no in-process shortcut — these tests drive the SAME service the
// TypeScript and Python suites drive.

#if os(macOS)

import Foundation
import ArchAstroPlatform

final class HarnessServiceClient: @unchecked Sendable {
    let wsURL: String
    let controlURL: String

    private let session: URLSession
    private let sockets = Locked<[Socket]>([])

    init(wsURL: String, controlURL: String, requestTimeout: TimeInterval = 5.0) {
        self.wsURL = wsURL
        var trimmed = controlURL
        while trimmed.hasSuffix("/") { trimmed.removeLast() }
        self.controlURL = trimmed
        let configuration = URLSessionConfiguration.default
        configuration.timeoutIntervalForRequest = requestTimeout
        self.session = URLSession(configuration: configuration)
    }

    /// Disconnect every socket opened through this client.
    func close() async {
        let open = sockets.withLock { list -> [Socket] in
            let copy = list
            list = []
            return copy
        }
        for socket in open {
            await socket.disconnect()
        }
    }

    // MARK: HTTP control

    /// Clear every scenario, observation, and handler error on the server.
    func reset() async throws {
        _ = try await post("/reset", body: nil, expecting: 200...299)
    }

    /// Register a per-topic channel scenario (see the harness README for
    /// the ScenarioRequest JSON shape).
    func registerScenario(_ scenario: JSONValue) async throws {
        _ = try await post("/scenarios", body: scenario, expecting: 201...201)
    }

    /// Register a per-route SSE stream scenario.
    func registerStreamScenario(_ scenario: JSONValue) async throws {
        _ = try await post("/stream-scenarios", body: scenario, expecting: 201...201)
    }

    /// Fetch inbound frames the server validated, optionally filtered.
    func observations(topic: String? = nil, event: String? = nil) async throws -> [JSONValue] {
        var components = URLComponents(string: "\(controlURL)/observations")!
        var items: [URLQueryItem] = []
        if let topic { items.append(URLQueryItem(name: "topic", value: topic)) }
        if let event { items.append(URLQueryItem(name: "event", value: event)) }
        if !items.isEmpty { components.queryItems = items }
        let (data, response) = try await session.data(from: components.url!)
        try Self.check(response, data: data, expecting: 200...299, context: "observations")
        let parsed = try JSONCoding.decoder.decode(JSONValue.self, from: data)
        return parsed.arrayValue ?? []
    }

    /// Fetch scenario handler errors recorded by the server.
    func handlerErrors() async throws -> [JSONValue] {
        let url = URL(string: "\(controlURL)/handler-errors")!
        let (data, response) = try await session.data(from: url)
        try Self.check(response, data: data, expecting: 200...299, context: "handler-errors")
        let parsed = try JSONCoding.decoder.decode(JSONValue.self, from: data)
        return parsed.arrayValue ?? []
    }

    // MARK: Socket lifecycle

    /// Open a fresh Phoenix socket to the service's WebSocket endpoint.
    /// `autoReconnect` stays off so a dropped test connection surfaces
    /// immediately instead of silently retrying.
    func openSocket() async throws -> Socket {
        let socket = Socket(url: wsURL, autoReconnect: false)
        try await socket.connect()
        sockets.withLock { $0.append(socket) }
        return socket
    }

    // MARK: Internals

    private func post(
        _ path: String,
        body: JSONValue?,
        expecting: ClosedRange<Int>
    ) async throws -> Data {
        let url = URL(string: "\(controlURL)\(path)")!
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        if let body {
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            request.httpBody = try JSONCoding.encoder.encode(body)
        }
        let (data, response) = try await session.data(for: request)
        try Self.check(response, data: data, expecting: expecting, context: path)
        return data
    }

    private static func check(
        _ response: URLResponse,
        data: Data,
        expecting: ClosedRange<Int>,
        context: String
    ) throws {
        guard let http = response as? HTTPURLResponse else {
            throw ContractTestError.harnessRequestFailed("\(context): non-HTTP response")
        }
        guard expecting.contains(http.statusCode) else {
            let body = String(decoding: data, as: UTF8.self)
            throw ContractTestError.harnessRequestFailed(
                "\(context): HTTP \(http.statusCode) \(body)"
            )
        }
    }
}

#endif
