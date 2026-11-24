// Copyright (c) 2026 ArchAstro Inc. Licensed under the MIT License.
// See LICENSE for details.

// Array query parameters go out as one value in the form Platform's API DSL
// casts to a list (`{:array, _}` in ArchAstro.Schema.Validation): a single
// item as itself, several as a JSON array. Repeated plain keys would keep
// only the last value on the server.

import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
import Testing
@testable import ArchAstroPlatform

@Suite struct ArrayQueryTests {
    /// The query items of the one request a `custom_objects.list` call sends.
    private func sentQuery(org: [String]?, user: [String]? = nil) async throws -> [URLQueryItem] {
        let host = "stub-\(UUID().uuidString.lowercased()).test"
        let client = PlatformClient(baseUrl: "https://\(host)", accessToken: "tok", session: StubURLProtocol.makeSession())
        // The stub's body is not a list page; only the request matters here.
        _ = try? await client.v1.customObjects.list(type: "t", user: user, org: org)
        let url = try #require(StubURLProtocol.requests(host: host).first?.url)
        return URLComponents(url: url, resolvingAgainstBaseURL: false)?.queryItems ?? []
    }

    @Test func one_item_goes_as_itself() async throws {
        let items = try await sentQuery(org: ["org_1"])
        #expect(items.filter { $0.name == "org" }.map(\.value) == ["org_1"])
    }

    @Test func several_items_go_as_one_json_array() async throws {
        let items = try await sentQuery(org: ["org_1", "org_2"], user: ["me"])
        let org = items.filter { $0.name == "org" }
        #expect(org.count == 1)
        let decoded = try JSONDecoder().decode([String].self, from: Data(try #require(org.first?.value).utf8))
        #expect(decoded == ["org_1", "org_2"])
        #expect(items.filter { $0.name == "user" }.map(\.value) == ["me"])
    }

    @Test func a_lone_item_that_looks_like_json_goes_as_json() async throws {
        let items = try await sentQuery(org: ["[x]"])
        let value = try #require(items.first { $0.name == "org" }?.value)
        #expect(try JSONDecoder().decode([String].self, from: Data(value.utf8)) == ["[x]"])
    }

    @Test func an_empty_array_sends_nothing() async throws {
        let items = try await sentQuery(org: [])
        #expect(!items.contains { $0.name == "org" })
    }
}
