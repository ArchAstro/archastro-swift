// Runtime: async HTTP client for the generated Platform SDK.
// This file is hand-maintained, not generated. Port of the Python SDK's
// archastro/platform/runtime/http_client.py.

import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

/// Path prefix the `pathPrefix` rewrite applies to, matching the generated
/// default API version.
public let defaultApiPrefix = "/api/v1"

/// Structured API error with status code, error code, and message.
public struct ApiError: Error, Sendable {
    public let status: Int
    public let errorCode: String
    public let message: String
    public let body: JSONValue?

    public init(status: Int, errorCode: String, message: String, body: JSONValue? = nil) {
        self.status = status
        self.errorCode = errorCode
        self.message = message
        self.body = body
    }
}

/// Raw (non-JSON) response payload from `rawResponse` operations.
public struct RawResponse: Sendable {
    public let content: Data
    public let mimeType: String

    public init(content: Data, mimeType: String) {
        self.content = content
        self.mimeType = mimeType
    }
}

/// One parsed Server-Sent Event.
public struct SSEEvent: Hashable, Sendable {
    public let event: String
    public let data: JSONValue

    public init(event: String, data: JSONValue) {
        self.event = event
        self.data = data
    }
}

/// Errors surfaced by client construction and socket helpers.
public enum PlatformClientError: Error, Sendable {
    case missingAccessToken(String)
    case loginFailed(String)
    case refreshFailed(String)
    case refreshOnlyViolation(String)
    case invalidURL(String)
    case connectionFailed(String)
}

/// Async HTTP client backing every generated resource method.
///
/// Handles auth headers, one-shot 401 token refresh (deduplicated across
/// concurrent requests), structured `ApiError` parsing, and SSE streaming.
public final class HttpClient: @unchecked Sendable {
    private let baseUrl: String
    private let getAccessToken: (@Sendable () -> String?)?
    private let pathPrefix: String?
    private let defaultHeaders: [String: String]
    private let refreshOnly: Bool
    private let session: URLSession

    private struct MutableState {
        var accessToken: String?
        var onRefreshToken: (@Sendable () async throws -> String)?
        var refreshTask: Task<String, any Error>?
    }

    private let state: Locked<MutableState>

    public init(
        baseUrl: String,
        accessToken: String? = nil,
        getAccessToken: (@Sendable () -> String?)? = nil,
        onRefreshToken: (@Sendable () async throws -> String)? = nil,
        pathPrefix: String? = nil,
        defaultHeaders: [String: String] = [:],
        refreshOnly: Bool = false
    ) {
        var trimmed = baseUrl
        while trimmed.hasSuffix("/") { trimmed.removeLast() }
        self.baseUrl = trimmed
        self.getAccessToken = getAccessToken
        self.pathPrefix = pathPrefix
        self.defaultHeaders = defaultHeaders
        self.refreshOnly = refreshOnly
        let configuration = URLSessionConfiguration.default
        configuration.timeoutIntervalForRequest = 30
        self.session = URLSession(configuration: configuration)
        self.state = Locked(
            MutableState(accessToken: accessToken, onRefreshToken: onRefreshToken)
        )
    }

    // MARK: Token management

    public func setAccessToken(_ token: String) {
        state.withLock { $0.accessToken = token }
    }

    public func currentAccessToken() -> String? {
        if let getAccessToken { return getAccessToken() }
        return state.withLock { $0.accessToken }
    }

    public func setRefreshHandler(_ handler: @escaping @Sendable () async throws -> String) {
        state.withLock { $0.onRefreshToken = handler }
    }

    // MARK: Requests

    /// Issue a request and decode the JSON body into `T`.
    ///
    /// A bodyless 204 decodes as `JSONValue.null` when `T == JSONValue`;
    /// for typed models it throws, since the operation promised a body.
    public func request<T: Decodable & Sendable>(
        _ path: String,
        method: String = "GET",
        body: (any Encodable & Sendable)? = nil,
        query: [(String, String)]? = nil,
        headers: [String: String]? = nil
    ) async throws -> T {
        let (data, response) = try await execute(
            path, method: method, body: body, query: query, headers: headers
        )
        if response.statusCode == 204 || data.isEmpty {
            if let null = JSONValue.null as? T { return null }
            throw ApiError(
                status: response.statusCode,
                errorCode: "empty_response",
                message: "Server returned no body for an operation that promises one"
            )
        }
        return try JSONCoding.decoder.decode(T.self, from: data)
    }

    /// Issue a request, discarding any response body.
    public func requestVoid(
        _ path: String,
        method: String = "GET",
        body: (any Encodable & Sendable)? = nil,
        query: [(String, String)]? = nil,
        headers: [String: String]? = nil
    ) async throws {
        _ = try await execute(path, method: method, body: body, query: query, headers: headers)
    }

    /// Issue a request and return the raw bytes + MIME type.
    public func requestRaw(
        _ path: String,
        method: String = "GET",
        body: (any Encodable & Sendable)? = nil,
        query: [(String, String)]? = nil,
        headers: [String: String]? = nil
    ) async throws -> RawResponse {
        let (data, response) = try await execute(
            path, method: method, body: body, query: query, headers: headers
        )
        let mimeType = response.value(forHTTPHeaderField: "Content-Type") ?? "text/plain"
        return RawResponse(content: data, mimeType: mimeType)
    }

    /// Open a Server-Sent Events stream, yielding parsed events.
    ///
    /// Throws `ApiError` into the stream on a non-2xx response before any
    /// event is delivered. (Token refresh is not retried mid-stream.)
    public func streamSSE(
        _ path: String,
        method: String = "GET",
        body: (any Encodable & Sendable)? = nil,
        query: [(String, String)]? = nil,
        headers: [String: String]? = nil
    ) -> AsyncThrowingStream<SSEEvent, any Error> {
        AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    try self.checkRefreshOnly(path)
                    let request = try self.buildRequest(
                        path, method: method, body: body, query: query,
                        headers: headers, accept: "text/event-stream"
                    )
                    let (bytes, response) = try await self.session.bytes(for: request)
                    guard let http = response as? HTTPURLResponse else {
                        throw PlatformClientError.connectionFailed("Non-HTTP response")
                    }
                    if http.statusCode >= 400 {
                        var errorData = Data()
                        for try await byte in bytes { errorData.append(byte) }
                        throw Self.parseApiError(errorData, status: http.statusCode)
                    }

                    var lineBuffer = Data()
                    var event: String?
                    var dataLines: [String] = []

                    func flush() {
                        if let parsed = Self.buildSSEEvent(event: event, dataLines: dataLines) {
                            continuation.yield(parsed)
                        }
                        event = nil
                        dataLines = []
                    }

                    for try await byte in bytes {
                        if byte == UInt8(ascii: "\n") {
                            var line = String(decoding: lineBuffer, as: UTF8.self)
                            lineBuffer.removeAll(keepingCapacity: true)
                            if line.hasSuffix("\r") { line.removeLast() }
                            if line.isEmpty {
                                flush()
                            } else if line.hasPrefix("event:") {
                                event = String(line.dropFirst(6)).trimmingCharacters(in: .whitespaces)
                            } else if line.hasPrefix("data:") {
                                dataLines.append(
                                    String(line.dropFirst(5)).trimmingCharacters(in: .whitespaces)
                                )
                            }
                        } else {
                            lineBuffer.append(byte)
                        }
                    }
                    flush()
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    // MARK: Internals

    private func checkRefreshOnly(_ path: String) throws {
        let authPrefix = "\(defaultApiPrefix)/auth/"
        if refreshOnly && !path.hasPrefix(authPrefix) {
            throw PlatformClientError.refreshOnlyViolation(
                "Refresh-only HTTP client cannot make requests outside \(authPrefix)"
            )
        }
    }

    private func transformPath(_ path: String) -> String {
        guard let pathPrefix else { return path }
        if path.hasPrefix(defaultApiPrefix) {
            return pathPrefix + path.dropFirst(defaultApiPrefix.count)
        }
        return path
    }

    private func buildRequest(
        _ path: String,
        method: String,
        body: (any Encodable & Sendable)?,
        query: [(String, String)]?,
        headers: [String: String]?,
        accept: String? = nil
    ) throws -> URLRequest {
        let urlString = baseUrl + transformPath(path)
        guard var components = URLComponents(string: urlString) else {
            throw PlatformClientError.invalidURL(urlString)
        }
        if let query, !query.isEmpty {
            components.queryItems = query.map { URLQueryItem(name: $0.0, value: $0.1) }
        }
        guard let url = components.url else {
            throw PlatformClientError.invalidURL(urlString)
        }

        var request = URLRequest(url: url)
        request.httpMethod = method

        for (key, value) in defaultHeaders {
            request.setValue(value, forHTTPHeaderField: key)
        }
        let sendsBody = body != nil && method != "GET" && method != "HEAD"
        if sendsBody || accept == nil {
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        }
        if let accept {
            request.setValue(accept, forHTTPHeaderField: "Accept")
        }
        if let token = currentAccessToken() {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }
        if let headers {
            for (key, value) in headers {
                request.setValue(value, forHTTPHeaderField: key)
            }
        }
        if sendsBody, let body {
            request.httpBody = try JSONCoding.encoder.encode(AnyEncodable(body))
        }
        return request
    }

    private func doFetch(
        _ path: String,
        method: String,
        body: (any Encodable & Sendable)?,
        query: [(String, String)]?,
        headers: [String: String]?
    ) async throws -> (Data, HTTPURLResponse) {
        let request = try buildRequest(path, method: method, body: body, query: query, headers: headers)
        let (data, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw PlatformClientError.connectionFailed("Non-HTTP response")
        }
        return (data, http)
    }

    /// Fetch with auth gate, one-shot 401 auto-refresh, and error handling.
    private func execute(
        _ path: String,
        method: String,
        body: (any Encodable & Sendable)?,
        query: [(String, String)]?,
        headers: [String: String]?
    ) async throws -> (Data, HTTPURLResponse) {
        try checkRefreshOnly(path)
        let authPrefix = "\(defaultApiPrefix)/auth/"

        var (data, response) = try await doFetch(
            path, method: method, body: body, query: query, headers: headers
        )

        // Auto-refresh: on 401, attempt one token refresh and retry.
        // Concurrent 401s piggyback on the same in-flight refresh task.
        if response.statusCode == 401 && !path.hasPrefix(authPrefix) {
            let refreshTask: Task<String, any Error>? = state.withLock { state in
                guard state.onRefreshToken != nil else { return nil }
                if let existing = state.refreshTask { return existing }
                guard let handler = state.onRefreshToken else { return nil }
                let task = Task { [weak self] in
                    defer { self?.state.withLock { $0.refreshTask = nil } }
                    return try await handler()
                }
                state.refreshTask = task
                return task
            }
            if let refreshTask {
                do {
                    let newToken = try await refreshTask.value
                    setAccessToken(newToken)
                    (data, response) = try await doFetch(
                        path, method: method, body: body, query: query, headers: headers
                    )
                } catch {
                    // Refresh failed — fall through and surface the original 401.
                }
            }
        }

        if response.statusCode >= 400 {
            throw Self.parseApiError(data, status: response.statusCode)
        }
        return (data, response)
    }

    static func parseApiError(_ data: Data, status: Int) -> ApiError {
        let raw = try? JSONCoding.decoder.decode(JSONValue.self, from: data)
        if let error = raw?["error"], let object = error.objectValue {
            let code = object["code"]?.stringValue ?? object["type"]?.stringValue ?? "unknown_error"
            let message = object["message"]?.stringValue ?? "HTTP \(status)"
            return ApiError(status: status, errorCode: code, message: message, body: raw)
        }
        let errorString = raw?["error"]?.stringValue
        let message = raw?["message"]?.stringValue ?? errorString ?? "HTTP \(status)"
        return ApiError(
            status: status,
            errorCode: errorString ?? "unknown_error",
            message: message,
            body: raw
        )
    }

    static func buildSSEEvent(event: String?, dataLines: [String]) -> SSEEvent? {
        if event == nil && dataLines.isEmpty { return nil }
        let raw = dataLines.joined(separator: "\n")
        let data: JSONValue
        if let parsed = try? JSONCoding.decoder.decode(JSONValue.self, from: Data(raw.utf8)) {
            data = parsed
        } else {
            data = .string(raw)
        }
        return SSEEvent(event: event ?? "message", data: data)
    }
}

/// Type-erasing Encodable wrapper for heterogeneous request bodies.
struct AnyEncodable: Encodable {
    let value: any Encodable

    init(_ value: any Encodable) {
        self.value = value
    }

    func encode(to encoder: any Encoder) throws {
        try value.encode(to: encoder)
    }
}
