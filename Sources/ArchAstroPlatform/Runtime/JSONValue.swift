// Copyright (c) 2026 ArchAstro Inc. Licensed under the MIT License.
// See LICENSE for details.

// Runtime: JSON value type for the generated Platform SDK.
// This file is hand-maintained, not generated.

import Foundation

/// A JSON value — the Swift analogue of the untyped payloads the Python and
/// TypeScript SDKs pass around as dicts/objects. Literal-expressible so
/// generated code and callers can write `["key": "value"]`, `1`, `true`, …
public enum JSONValue: Codable, Hashable, Sendable {
    case null
    case bool(Bool)
    case int(Int)
    case double(Double)
    case string(String)
    case array([JSONValue])
    case object([String: JSONValue])

    // MARK: Codable

    public init(from decoder: any Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() {
            self = .null
        } else if let bool = try? container.decode(Bool.self) {
            self = .bool(bool)
        } else if let int = try? container.decode(Int.self) {
            self = .int(int)
        } else if let double = try? container.decode(Double.self) {
            self = .double(double)
        } else if let string = try? container.decode(String.self) {
            self = .string(string)
        } else if let array = try? container.decode([JSONValue].self) {
            self = .array(array)
        } else if let object = try? container.decode([String: JSONValue].self) {
            self = .object(object)
        } else {
            throw DecodingError.dataCorrupted(
                .init(codingPath: decoder.codingPath, debugDescription: "Unsupported JSON value")
            )
        }
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .null: try container.encodeNil()
        case .bool(let value): try container.encode(value)
        case .int(let value): try container.encode(value)
        case .double(let value): try container.encode(value)
        case .string(let value): try container.encode(value)
        case .array(let value): try container.encode(value)
        case .object(let value): try container.encode(value)
        }
    }

    // MARK: Accessors

    public subscript(key: String) -> JSONValue? {
        if case .object(let object) = self { return object[key] }
        return nil
    }

    public subscript(index: Int) -> JSONValue? {
        if case .array(let array) = self, array.indices.contains(index) {
            return array[index]
        }
        return nil
    }

    public var isNull: Bool {
        if case .null = self { return true }
        return false
    }

    public var boolValue: Bool? {
        if case .bool(let value) = self { return value }
        return nil
    }

    public var intValue: Int? {
        switch self {
        case .int(let value): return value
        case .double(let value) where value.truncatingRemainder(dividingBy: 1) == 0:
            return Int(value)
        default: return nil
        }
    }

    public var doubleValue: Double? {
        switch self {
        case .int(let value): return Double(value)
        case .double(let value): return value
        default: return nil
        }
    }

    public var stringValue: String? {
        if case .string(let value) = self { return value }
        return nil
    }

    public var arrayValue: [JSONValue]? {
        if case .array(let value) = self { return value }
        return nil
    }

    public var objectValue: [String: JSONValue]? {
        if case .object(let value) = self { return value }
        return nil
    }

    /// Re-decode this JSON value into a typed `Decodable`.
    public func decode<T: Decodable>(_ type: T.Type = T.self) throws -> T {
        let data = try JSONCoding.encoder.encode(self)
        return try JSONCoding.decode(T.self, from: data)
    }

    /// Build a `JSONValue` from any `Encodable` (via a JSON round-trip).
    public init(encodable value: some Encodable) throws {
        let data = try JSONCoding.encoder.encode(value)
        self = try JSONCoding.decoder.decode(JSONValue.self, from: data)
    }

    /// Wire string for query parameters: bare strings stay unquoted;
    /// everything else is compact JSON.
    public var queryString: String {
        if case .string(let value) = self { return value }
        let data = (try? JSONCoding.encoder.encode(self)) ?? Data()
        return String(decoding: data, as: UTF8.self)
    }
}

// MARK: - Literals

extension JSONValue: ExpressibleByBooleanLiteral {
    public init(booleanLiteral value: Bool) { self = .bool(value) }
}

extension JSONValue: ExpressibleByIntegerLiteral {
    public init(integerLiteral value: Int) { self = .int(value) }
}

extension JSONValue: ExpressibleByFloatLiteral {
    public init(floatLiteral value: Double) { self = .double(value) }
}

extension JSONValue: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) { self = .string(value) }
}

extension JSONValue: ExpressibleByArrayLiteral {
    public init(arrayLiteral elements: JSONValue...) { self = .array(elements) }
}

extension JSONValue: ExpressibleByDictionaryLiteral {
    public init(dictionaryLiteral elements: (String, JSONValue)...) {
        self = .object(Dictionary(elements, uniquingKeysWith: { _, last in last }))
    }
}

// MARK: - JSON coding configuration

/// Shared encoder/decoder with the SDK's datetime conventions. Decoding
/// accepts ISO-8601 timestamps with optional fractional seconds and treats
/// timestamps without an explicit timezone as UTC.
public enum JSONCoding {
    nonisolated(unsafe) private static let isoFormatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        return formatter
    }()

    nonisolated(unsafe) private static let isoFractionalFormatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()

    public static func isoString(from date: Date) -> String {
        isoFormatter.string(from: date)
    }

    public static func parseDate(_ string: String) -> Date? {
        if let date = isoFractionalFormatter.date(from: string)
            ?? isoFormatter.date(from: string)
        {
            return date
        }

        // Some platform resources historically emitted database timestamps
        // without a timezone suffix. They represent UTC, so normalize them at
        // the SDK boundary rather than forcing every client to decode around
        // otherwise valid generated models.
        let assumedUTC = string + "Z"
        return isoFractionalFormatter.date(from: assumedUTC)
            ?? isoFormatter.date(from: assumedUTC)
    }

    public static var decoder: JSONDecoder {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .custom { decoder in
            let container = try decoder.singleValueContainer()
            let string = try container.decode(String.self)
            guard let date = parseDate(string) else {
                throw DecodingError.dataCorrupted(
                    .init(
                        codingPath: decoder.codingPath,
                        debugDescription: "Invalid ISO-8601 date: \(string)"
                    )
                )
            }
            return date
        }
        return decoder
    }

    /// Decode a generated SDK response while normalizing legacy platform wire
    /// shapes that predate the current expanded-object schema.
    public static func decode<T: Decodable>(
        _ type: T.Type,
        from data: Data
    ) throws -> T {
        do {
            return try decoder.decode(T.self, from: data)
        } catch {
            let normalized: Data?
            if T.self == TeamThreadListResponse.self {
                normalized = normalizeTeamThreadCreators(in: data)
            } else if T.self == ThreadMessagesResponse.self {
                normalized = normalizeThreadMessageUsers(in: data)
            } else if T.self == ApiChatMessageAddedPayload.self
                || T.self == ApiChatMessageUpdatedPayload.self
            {
                normalized = normalizeChannelMessageUser(in: data)
            } else {
                normalized = nil
            }
            guard let normalized else {
                throw error
            }
            return try decoder.decode(T.self, from: normalized)
        }
    }

    private static func normalizeTeamThreadCreators(in data: Data) -> Data? {
        guard
            var root = try? JSONSerialization.jsonObject(with: data)
                as? [String: Any],
            var items = root["data"] as? [[String: Any]]
        else {
            return nil
        }

        var changed = false
        for index in items.indices {
            if let creatorID = items[index]["creator"] as? String {
                items[index]["creator"] = ["id": creatorID]
                changed = true
            }
        }
        guard changed else { return nil }
        root["data"] = items
        return try? JSONSerialization.data(withJSONObject: root)
    }

    private static func normalizeThreadMessageUsers(in data: Data) -> Data? {
        guard
            var root = try? JSONSerialization.jsonObject(with: data)
                as? [String: Any],
            var envelope = root["data"] as? [String: Any],
            var messages = envelope["messages"] as? [[String: Any]]
        else {
            return nil
        }

        let changed = messages.indices.reduce(into: false) { changed, index in
            changed = normalizeMessageUser(in: &messages[index]) || changed
        }
        guard changed else { return nil }
        envelope["messages"] = messages
        root["data"] = envelope
        return try? JSONSerialization.data(withJSONObject: root)
    }

    private static func normalizeChannelMessageUser(in data: Data) -> Data? {
        guard
            var root = try? JSONSerialization.jsonObject(with: data)
                as? [String: Any],
            var message = root["message"] as? [String: Any],
            normalizeMessageUser(in: &message)
        else {
            return nil
        }

        root["message"] = message
        return try? JSONSerialization.data(withJSONObject: root)
    }

    private static func normalizeMessageUser(
        in message: inout [String: Any]
    ) -> Bool {
        var changed = false
        if
            let expandedUser = message["user"] as? [String: Any],
            let userID = expandedUser["id"] as? String
        {
            message["user"] = userID
            changed = true
        }

        if var reactions = message["reactions"] as? [[String: Any]] {
            for index in reactions.indices {
                if
                    let expandedUser = reactions[index]["user"] as? [String: Any],
                    let userID = expandedUser["id"] as? String
                {
                    reactions[index]["user"] = userID
                    changed = true
                }
            }
            message["reactions"] = reactions
        }
        return changed
    }

    public static var encoder: JSONEncoder {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .custom { date, encoder in
            var container = encoder.singleValueContainer()
            try container.encode(isoString(from: date))
        }
        return encoder
    }
}

/// Minimal lock-protected box for mutable state on Sendable classes.
public final class Locked<Value>: @unchecked Sendable {
    private let lock = NSLock()
    private var value: Value

    public init(_ value: Value) {
        self.value = value
    }

    public func withLock<Result>(_ body: (inout Value) throws -> Result) rethrows -> Result {
        lock.lock()
        defer { lock.unlock() }
        return try body(&value)
    }
}
