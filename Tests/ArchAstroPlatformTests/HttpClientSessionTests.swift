// Copyright (c) 2026 ArchAstro Inc. Licensed under the MIT License.
// See LICENSE for details.

// Tests for injecting a caller-owned URLSession into HttpClient. Requests
// are served by a URLProtocol stub registered on an ephemeral session
// configuration, so nothing touches the network.

import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
import Testing
@testable import ArchAstroPlatform

/// Serves canned responses and records every request it sees. Each test
/// uses its own host so parallel tests never read each other's records.
final class StubURLProtocol: URLProtocol, @unchecked Sendable {
    struct Recorded: Sendable {
        let url: URL
        let method: String
        let headers: [String: String]
    }

    static let recorded = Locked<[Recorded]>([])

    static func requests(host: String) -> [Recorded] {
        recorded.withLock { $0.filter { $0.url.host == host } }
    }

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let url = request.url else { return }
        let headers = request.allHTTPHeaderFields ?? [:]
        Self.recorded.withLock {
            $0.append(Recorded(url: url, method: request.httpMethod ?? "GET", headers: headers))
        }
        // `/api/v1/protected` answers 401 until it sees the refreshed token.
        let status: Int
        let body: Data
        if url.path == "/api/v1/protected" && headers["Authorization"] != "Bearer fresh" {
            status = 401
            body = Data(#"{"error":{"code":"unauthorized","message":"expired"}}"#.utf8)
        } else {
            status = 200
            body = Data(#"{"ok":true,"path":"\#(url.path)"}"#.utf8)
        }
        let response = HTTPURLResponse(
            url: url,
            statusCode: status,
            httpVersion: "HTTP/1.1",
            headerFields: ["Content-Type": "application/json"]
        )!
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: body)
        client?.urlProtocolDidFinishLoading(self)
    }

    override func stopLoading() {}

    static func makeSession() -> URLSession {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [StubURLProtocol.self]
        return URLSession(configuration: configuration)
    }
}

@Suite struct HttpClientSessionTests {
    private func uniqueHost() -> String {
        "stub-\(UUID().uuidString.lowercased()).test"
    }

    @Test func requests_go_through_the_injected_session() async throws {
        let host = uniqueHost()
        let session = StubURLProtocol.makeSession()
        let client = HttpClient(
            baseUrl: "https://\(host)/",
            accessToken: "tok",
            defaultHeaders: ["x-archastro-api-key": "pk_test"],
            session: session
        )
        #expect(client.session === session)

        let value: JSONValue = try await client.request("/api/v1/ping", query: [("a", "1")])

        #expect(value["ok"] == true)
        #expect(value["path"] == "/api/v1/ping")
        let seen = StubURLProtocol.requests(host: host)
        #expect(seen.count == 1)
        #expect(seen.first?.method == "GET")
        #expect(seen.first?.url.query == "a=1")
        #expect(seen.first?.headers["Authorization"] == "Bearer tok")
        #expect(seen.first?.headers["x-archastro-api-key"] == "pk_test")
    }

    @Test func refresh_on_401_retries_through_the_injected_session() async throws {
        let host = uniqueHost()
        let client = HttpClient(
            baseUrl: "https://\(host)",
            accessToken: "stale",
            onRefreshToken: { "fresh" },
            session: StubURLProtocol.makeSession()
        )

        let value: JSONValue = try await client.request("/api/v1/protected")

        #expect(value["ok"] == true)
        let auth = StubURLProtocol.requests(host: host).map { $0.headers["Authorization"] }
        #expect(auth == ["Bearer stale", "Bearer fresh"])
    }

    @Test func injected_session_configuration_is_left_untouched() {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.timeoutIntervalForRequest = 7
        let session = URLSession(configuration: configuration)
        let client = HttpClient(baseUrl: "https://example.test", session: session)
        #expect(client.session === session)
        #expect(client.session.configuration.timeoutIntervalForRequest == 7)
    }

    @Test func default_session_is_private_with_a_30s_request_timeout() {
        let client = HttpClient(baseUrl: "https://example.test")
        #expect(client.session !== URLSession.shared)
        #expect(client.session.configuration.timeoutIntervalForRequest == 30)
        let other = HttpClient(baseUrl: "https://example.test")
        #expect(other.session !== client.session)
    }
}
