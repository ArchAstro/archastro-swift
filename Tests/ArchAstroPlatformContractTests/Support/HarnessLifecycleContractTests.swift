// Copyright (c) 2026 ArchAstro Inc. Licensed under the MIT License.
// See LICENSE for details.

#if os(macOS)

import Foundation
import Testing
import ArchAstroPlatform

/// Proves harness start finishes via HTTP /health rather than blocking the
/// test process on child stdout. The previous FileHandle.availableData poll
/// hung `swift test` on GitHub-hosted macOS until the runner died.
@Suite(.serialized, .enabled(if: ContractSupport.channelTestsEnabled))
struct HarnessLifecycleContractTests {
    @Test func harness_control_health_answers_after_start() async throws {
        try await ContractSupport.withHarness { client in
            let url = URL(string: "\(client.controlURL)/health")!
            var request = URLRequest(url: url)
            request.timeoutInterval = 2
            let (data, response) = try await URLSession.shared.data(for: request)
            let http = try #require(response as? HTTPURLResponse)
            #expect(http.statusCode == 200)
            let body = try JSONCoding.decoder.decode(JSONValue.self, from: data)
            #expect(body["ok"]?.boolValue == true)
            #expect(body["wsUrl"]?.stringValue == client.wsURL)
        }
    }
}

#endif
