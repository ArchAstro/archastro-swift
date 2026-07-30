// Copyright (c) 2026 ArchAstro Inc. Licensed under the MIT License.
// See LICENSE for details.

// Unit tests for the hand-maintained runtime (JSONValue, JSON coding,
// error parsing, SSE framing). The contract tests cover the generated
// surface; these cover runtime behavior that doesn't need a server.

import Foundation
import Testing
@testable import ArchAstroPlatform

@Suite struct JSONValueTests {
    @Test func literals_build_expected_cases() {
        let value: JSONValue = [
            "name": "Acme",
            "count": 3,
            "ratio": 1.5,
            "active": true,
            "tags": ["a", "b"],
            "meta": [:],
        ]
        #expect(value["name"] == "Acme")
        #expect(value["count"] == 3)
        #expect(value["ratio"] == 1.5)
        #expect(value["active"] == true)
        #expect(value["tags"]?[1] == "b")
        #expect(value["meta"] == JSONValue.object([:]))
        #expect(value["missing"] == nil)
    }

    @Test func round_trips_through_codable() throws {
        let original: JSONValue = [
            "nested": ["list": [1, 2.5, true, "x", JSONValue.null]]
        ]
        let data = try JSONCoding.encoder.encode(original)
        let decoded = try JSONCoding.decoder.decode(JSONValue.self, from: data)
        #expect(decoded == original)
    }

    @Test func accessors_convert_between_numeric_cases() {
        #expect(JSONValue.int(3).doubleValue == 3.0)
        #expect(JSONValue.double(3.0).intValue == 3)
        #expect(JSONValue.double(3.5).intValue == nil)
        #expect(JSONValue.string("x").intValue == nil)
    }

    @Test func query_string_keeps_bare_strings_and_encodes_the_rest() {
        #expect(JSONValue.string("plain").queryString == "plain")
        #expect(JSONValue.object(["a": .int(1)]).queryString == #"{"a":1}"#)
        #expect(JSONValue.bool(true).queryString == "true")
    }

    @Test func typed_decode_from_json_value() throws {
        struct Point: Codable, Equatable {
            var x: Int
            var y: Int
        }
        let value: JSONValue = ["x": 1, "y": 2]
        let point: Point = try value.decode()
        #expect(point == Point(x: 1, y: 2))
    }
}

@Suite struct JSONCodingTests {
    @Test func parses_iso_dates_with_optional_fractional_seconds_and_timezones() {
        let expected = Date(timeIntervalSince1970: 1_704_067_200)

        #expect(JSONCoding.parseDate("2024-01-01T00:00:00Z") == expected)
        #expect(JSONCoding.parseDate("2024-01-01T00:00:00.000Z") == expected)
        #expect(JSONCoding.parseDate("2024-01-01T01:00:00+01:00") == expected)
        #expect(JSONCoding.parseDate("2024-01-01T00:00:00") == expected)
        #expect(JSONCoding.parseDate("2024-01-01T00:00:00.000000") == expected)
        #expect(JSONCoding.parseDate("not-a-date") == nil)
    }

    @Test func decodes_dates_inside_models() throws {
        struct Stamped: Codable {
            var at: Date
        }
        let json = #"{"at":"2024-06-01T12:30:00.500Z"}"#
        let decoded = try JSONCoding.decoder.decode(Stamped.self, from: Data(json.utf8))
        #expect(decoded.at.timeIntervalSince1970 > 0)
    }

    @Test func decodes_production_timestamp_without_timezone_inside_models() throws {
        struct Stamped: Codable {
            var at: Date
        }
        let json = #"{"at":"2026-07-23T01:16:47"}"#
        let decoded = try JSONCoding.decoder.decode(Stamped.self, from: Data(json.utf8))
        #expect(
            JSONCoding.isoString(from: decoded.at) == "2026-07-23T01:16:47Z"
        )
    }

    @Test func encodes_dates_as_iso_strings() throws {
        struct Stamped: Codable {
            var at: Date
        }
        let data = try JSONCoding.encoder.encode(Stamped(at: Date(timeIntervalSince1970: 0)))
        let text = String(decoding: data, as: UTF8.self)
        #expect(text.contains("1970-01-01T00:00:00Z"))
    }
}

@Suite struct GeneratedResponseCompatibilityTests {
    @Test func team_thread_creator_accepts_an_unexpanded_user_id() throws {
        let response = try JSONCoding.decode(
            TeamThreadListResponse.self,
            from: Data(
                """
                {
                  "data": [{
                    "id": "thr_room",
                    "creator": "usr_creator"
                  }]
                }
                """.utf8
            )
        )

        #expect(response.data.first?.creator?.id == "usr_creator")
    }

    @Test func team_thread_creator_still_accepts_an_expanded_user() throws {
        let response = try JSONCoding.decode(
            TeamThreadListResponse.self,
            from: Data(
                """
                {
                  "data": [{
                    "id": "thr_room",
                    "creator": {
                      "id": "usr_creator",
                      "name": "Creator"
                    }
                  }]
                }
                """.utf8
            )
        )

        #expect(response.data.first?.creator?.id == "usr_creator")
        #expect(response.data.first?.creator?.name == "Creator")
    }
}

@Suite struct ApiErrorParsingTests {
    private func parse(_ json: String, status: Int) -> ApiError {
        HttpClient.parseApiError(Data(json.utf8), status: status)
    }

    @Test func parses_structured_error_objects() {
        let error = parse(
            #"{"error":{"code":"not_found","message":"Agent missing"}}"#, status: 404
        )
        #expect(error.status == 404)
        #expect(error.errorCode == "not_found")
        #expect(error.message == "Agent missing")
    }

    @Test func falls_back_to_type_when_code_missing() {
        let error = parse(#"{"error":{"type":"validation_error"}}"#, status: 422)
        #expect(error.errorCode == "validation_error")
        #expect(error.message == "HTTP 422")
    }

    @Test func handles_string_errors_and_bare_messages() {
        let stringError = parse(#"{"error":"boom"}"#, status: 400)
        #expect(stringError.errorCode == "boom")
        #expect(stringError.message == "boom")

        let messageOnly = parse(#"{"message":"nope"}"#, status: 403)
        #expect(messageOnly.errorCode == "unknown_error")
        #expect(messageOnly.message == "nope")
    }

    @Test func handles_non_json_bodies() {
        let error = parse("<html>oops</html>", status: 500)
        #expect(error.errorCode == "unknown_error")
        #expect(error.message == "HTTP 500")
    }
}

@Suite struct SSEFramingTests {
    @Test func builds_events_from_event_and_data_lines() {
        let event = HttpClient.buildSSEEvent(
            event: "message_delta", dataLines: [#"{"text":"hi"}"#]
        )
        #expect(event?.event == "message_delta")
        #expect(event?.data["text"] == "hi")
    }

    @Test func defaults_event_name_and_keeps_raw_strings() {
        let event = HttpClient.buildSSEEvent(event: nil, dataLines: ["not json"])
        #expect(event?.event == "message")
        #expect(event?.data == "not json")
    }

    @Test func empty_frames_produce_nothing() {
        #expect(HttpClient.buildSSEEvent(event: nil, dataLines: []) == nil)
    }

    @Test func multi_line_data_joins_with_newlines() {
        let event = HttpClient.buildSSEEvent(event: "chunk", dataLines: ["a", "b"])
        #expect(event?.data == "a\nb")
    }
}
