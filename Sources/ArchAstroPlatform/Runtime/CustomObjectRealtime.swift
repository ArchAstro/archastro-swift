// Copyright (c) 2026 ArchAstro Inc. Licensed under the MIT License.
// See LICENSE for details.

// Runtime: value types, wire parsing and error classification for managed
// custom-object subscriptions (hand-maintained). Port of the helpers in
// @archastro/sdk custom-object-subscriptions.ts. The subscription itself is
// CustomObjectSubscription.swift.
//
// Errors are classified by the server's reply `reason` codes
// (api_object_channel.ex) and HTTP/close status, never by message text.

import Foundation

/// Connection lifecycle of a managed subscription.
public enum CustomObjectConnectionState: String, Sendable, Equatable {
    case connecting, live, reconnecting, offline, unauthorized, closed
}

public struct CustomObjectSnapshot: Sendable, Equatable {
    public let id: String
    public let fields: [String: JSONValue]
    public let readonly: Bool?
    public let connectionId: String?
}

/// A server update. With `partial`, `fields` is a deep-merge patch;
/// otherwise it is the whole field map. `operationId` matches the one this
/// client sent for its own updates.
public struct CustomObjectUpdate: Sendable, Equatable {
    public let id: String
    public let fields: [String: JSONValue]
    public let operationId: String?
    public let partial: Bool
}

public struct CustomObjectCursor: Sendable, Equatable {
    public var x: Double
    public var y: Double

    public init(x: Double, y: Double) {
        self.x = x
        self.y = y
    }
}

public enum CustomObjectActivity: String, Sendable, Equatable {
    case active, idle
}

public struct CustomObjectPresence: Sendable, Equatable {
    public let userId: String
    public let connectionId: String
    public let displayName: String
    public let cursor: CustomObjectCursor?
    public let selectedElementIds: [String]
    public let color: String
    public let activity: CustomObjectActivity
}

public struct CustomObjectPresenceUpdate: Sendable, Equatable {
    public var cursor: CustomObjectCursor?
    public var selectedElementIds: [String]
    public var activity: CustomObjectActivity

    public init(cursor: CustomObjectCursor?, selectedElementIds: [String], activity: CustomObjectActivity) {
        self.cursor = cursor
        self.selectedElementIds = selectedElementIds
        self.activity = activity
    }

    var wire: JSONValue {
        [
            "cursor": cursor.map { ["x": .double($0.x), "y": .double($0.y)] } ?? .null,
            "selected_element_ids": .array(selectedElementIds.map { .string($0) }),
            "state": .string(activity.rawValue),
        ]
    }
}

public struct CustomObjectPresenceLeave: Sendable, Equatable {
    public let connectionId: String
}

/// Why a subscription operation failed. Mirrors the TS SDK's ApiError
/// classes; `code` is the server's reason code where there is one.
public enum CustomObjectSubscriptionError: Error, Sendable, Equatable {
    /// 401: the socket handshake refused the token.
    case authentication(code: String)
    /// 403: `unauthorized` reply, handshake 403, or `access_revoked`.
    case authorization(code: String)
    /// 404: `not_found`/`deleted` reply or `object_deleted`.
    case notFound(code: String)
    /// 422: malformed payload, full offline queue, or `validation` reply.
    case validation(code: String, details: JSONValue?)
    /// The server refused this request (`readonly`, `invalid_operation_id`,
    /// ...); the subscription stays live.
    case rejected(code: String)
    /// Transport trouble; the subscription reconnects.
    case network(String)

    /// Errors after which the subscription cannot continue.
    public var isTerminal: Bool {
        switch self {
        case .authentication, .authorization, .notFound: true
        case .validation, .rejected, .network: false
        }
    }

    /// The state a terminal error leaves the subscription in.
    var terminalState: CustomObjectConnectionState {
        switch self {
        case .authentication, .authorization: .unauthorized
        default: .closed
        }
    }

    /// The reason code of a failed reply. The server's catch-all replies
    /// `inspect(atom)`, so `:rate_limited` and `rate_limited` are one code.
    static func reasonCode(_ response: JSONValue) -> String {
        let raw = response["reason"]?.stringValue ?? response["code"]?.stringValue
            ?? response["error"]?.stringValue ?? response.stringValue ?? "unknown"
        return raw.hasPrefix(":") ? String(raw.dropFirst()) : raw
    }

    /// A failed reply `{reason: ...}` from the object channel.
    static func fromReply(_ response: JSONValue) -> CustomObjectSubscriptionError {
        let reason = reasonCode(response)
        switch reason {
        case "unauthorized", "forbidden": return .authorization(code: reason)
        case "not_found", "deleted": return .notFound(code: reason)
        case "validation": return .validation(code: reason, details: response["validation_errors"])
        // The object's server process went away; it comes back on rejoin.
        case "object_unavailable", "room_unavailable": return .network(reason)
        default: return .rejected(code: reason)
        }
    }

    /// Anything thrown while connecting, joining or pushing.
    static func classify(_ error: any Error) -> CustomObjectSubscriptionError {
        switch error {
        case let classified as CustomObjectSubscriptionError:
            return classified
        case let rejected as PhxTransportRejected:
            switch rejected.status {
            case 401: return .authentication(code: "unauthorized")
            case 403: return .authorization(code: "forbidden")
            case 404: return .notFound(code: "not_found")
            default: return .network("WebSocket upgrade failed with HTTP \(rejected.status)")
            }
        case ChannelError.joinRejected(let response):
            return fromReply(response)
        case let api as ApiError:
            switch api.status {
            case 401: return .authentication(code: api.errorCode)
            case 403: return .authorization(code: api.errorCode)
            case 404: return .notFound(code: api.errorCode)
            case 422: return .validation(code: api.errorCode, details: api.body)
            default: return .network("HTTP \(api.status)")
            }
        default:
            return .network(String(describing: error))
        }
    }
}

// MARK: - Wire parsing

enum CustomObjectWire {
    /// `{id, fields}` envelope shared by join replies and updates.
    static func envelope(_ payload: JSONValue?) -> (id: String, fields: [String: JSONValue])? {
        guard let id = payload?["id"]?.stringValue, let fields = payload?["fields"]?.objectValue else {
            return nil
        }
        return (id, fields)
    }

    static func presence(_ payload: JSONValue?) -> CustomObjectPresence? {
        guard let payload, payload.objectValue != nil else { return nil }
        guard
            let userId = field(payload, "user_id", "userId"),
            let connectionId = field(payload, "connection_id", "connectionId"),
            let displayName = field(payload, "display_name", "displayName"),
            let selected = (payload["selected_element_ids"] ?? payload["selectedElementIds"])?.arrayValue
        else { return nil }
        let ids = selected.compactMap(\.stringValue)
        guard ids.count == selected.count else { return nil }
        let activity = (payload["state"]?.stringValue).flatMap(CustomObjectActivity.init(rawValue:))
            ?? (payload["activity"]?.stringValue).flatMap(CustomObjectActivity.init(rawValue:))
        guard let activity else { return nil }
        var cursor: CustomObjectCursor?
        if let raw = payload["cursor"], raw != .null {
            guard let x = raw["x"]?.doubleValue, let y = raw["y"]?.doubleValue else { return nil }
            cursor = CustomObjectCursor(x: x, y: y)
        }
        return CustomObjectPresence(
            userId: userId,
            connectionId: connectionId,
            displayName: displayName,
            cursor: cursor,
            selectedElementIds: ids,
            color: payload["color"]?.stringValue ?? presenceColor(userId),
            activity: activity
        )
    }

    static func presenceLeave(_ payload: JSONValue?) -> CustomObjectPresenceLeave? {
        guard let payload, let id = field(payload, "connection_id", "connectionId") else { return nil }
        return CustomObjectPresenceLeave(connectionId: id)
    }

    private static func field(_ value: JSONValue, _ snake: String, _ camel: String) -> String? {
        (value[snake] ?? value[camel])?.stringValue
    }

    /// Same palette and hash as the TS SDK, so web and native agree.
    static func presenceColor(_ identity: String) -> String {
        let palette = ["#89b4fa", "#a6e3a1", "#f9e2af", "#f38ba8", "#cba6f7", "#94e2d5", "#fab387"]
        var hash: UInt32 = 0
        for unit in identity.utf16 {
            hash = hash &* 31 &+ UInt32(unit)
        }
        return palette[Int(hash % UInt32(palette.count))]
    }

    /// Objects merge key by key; anything else replaces (the server's rule).
    static func deepMerge(_ left: [String: JSONValue], _ right: [String: JSONValue]) -> [String: JSONValue] {
        var merged = left
        for (key, value) in right {
            if case .object(let existing)? = merged[key], case .object(let incoming) = value {
                merged[key] = .object(deepMerge(existing, incoming))
            } else {
                merged[key] = value
            }
        }
        return merged
    }
}

// MARK: - One-shot result

/// A result settled once and awaited by any number of callers. The actor
/// registers it synchronously, then awaits outside its own state, which a
/// continuation body cannot do.
final class SubscriptionPromise: Sendable {
    private struct State {
        var result: Result<Void, CustomObjectSubscriptionError>?
        var waiters: [CheckedContinuation<Void, any Error>] = []
    }

    private let state = Locked(State())

    func resolve() { settle(.success(())) }

    func reject(_ error: CustomObjectSubscriptionError) { settle(.failure(error)) }

    private func settle(_ result: Result<Void, CustomObjectSubscriptionError>) {
        let waiters = state.withLock { state -> [CheckedContinuation<Void, any Error>] in
            guard state.result == nil else { return [] }
            state.result = result
            defer { state.waiters = [] }
            return state.waiters
        }
        for waiter in waiters { waiter.resume(with: result.mapError { $0 as any Error }) }
    }

    func value() async throws {
        try await withCheckedThrowingContinuation { (waiter: CheckedContinuation<Void, any Error>) in
            let settled = state.withLock { state -> Result<Void, CustomObjectSubscriptionError>? in
                if state.result == nil { state.waiters.append(waiter) }
                return state.result
            }
            if let settled { waiter.resume(with: settled.mapError { $0 as any Error }) }
        }
    }
}
