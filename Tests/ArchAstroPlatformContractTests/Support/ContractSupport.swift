// Copyright (c) 2026 ArchAstro Inc. Licensed under the MIT License.
// See LICENSE for details.

// Test support: Prism + channel-harness lifecycle for contract tests.
// This file is hand-maintained, not generated — the Swift analogue of the
// generated Python conftest.py plus the handwritten HarnessServiceClient.

#if os(macOS)

import Foundation
import Testing
@testable import ArchAstroPlatform

/// Parse an ISO-8601 string into a Date for generated test arguments.
func isoDate(_ string: String) -> Date {
    JSONCoding.parseDate(string) ?? Date(timeIntervalSince1970: 0)
}

enum ContractTestError: Error {
    case prismStartFailed(String)
    case harnessStartFailed(String)
    case harnessRequestFailed(String)
    case timeout(String)
}

enum ContractSupport {
    // MARK: Environment

    static var packageRoot: URL {
        // …/Tests/ArchAstroPlatformContractTests/Support/ContractSupport.swift
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }

    static var prismPort: String {
        ProcessInfo.processInfo.environment["PRISM_PORT"] ?? "4040"
    }

    static var prismURL: String { "http://127.0.0.1:\(prismPort)" }

    static var specPath: String {
        ProcessInfo.processInfo.environment["OPENAPI_SPEC_PATH"]
            ?? packageRoot.appendingPathComponent("specs/platform-openapi.json").path
    }

    static var prismBin: String {
        ProcessInfo.processInfo.environment["PRISM_BIN"]
            ?? packageRoot.appendingPathComponent("node_modules/.bin/prism").path
    }

    static var harnessBin: String {
        ProcessInfo.processInfo.environment["ARCHASTRO_HARNESS_BIN"]
            ?? packageRoot
                .appendingPathComponent("node_modules/@archastro/channel-harness/dist/bin.js")
                .path
    }

    /// Channel/stream contract tests are opt-in, mirroring the Python and
    /// TypeScript suites.
    static var channelTestsEnabled: Bool {
        let value = ProcessInfo.processInfo.environment["ARCHASTRO_RUN_CHANNEL_CONTRACT_TESTS"] ?? ""
        return ["1", "true", "yes"].contains(value.lowercased())
    }

    // MARK: Clients

    static func client() async throws -> PlatformClient {
        try await TestServers.shared.ensurePrism()
        return PlatformClient(
            baseUrl: prismURL,
            accessToken: "test-token",
            defaultHeaders: ["x-archastro-api-key": "pk_test-key"]
        )
    }

    static func errorClient(_ code: Int) async throws -> PlatformClient {
        try await TestServers.shared.ensurePrism()
        return PlatformClient(
            baseUrl: prismURL,
            accessToken: "test-token",
            defaultHeaders: [
                "x-archastro-api-key": "pk_test-key",
                "Prefer": "code=\(code)",
            ]
        )
    }

    /// Sink for generated assertions that only need the value to exist.
    static func use<T>(_ value: T) {
        _ = value
    }

    // MARK: Harness access

    /// Run `body` with an exclusive, reset harness-service client.
    /// Serialized globally — harness scenarios are cross-test shared state.
    static func withHarness(
        _ body: @Sendable (HarnessServiceClient) async throws -> Void
    ) async throws {
        await HarnessGate.shared.acquire()
        do {
            let urls = try await TestServers.shared.ensureHarness()
            let client = HarnessServiceClient(wsURL: urls.wsUrl, controlURL: urls.controlUrl)
            do {
                try await client.reset()
                try await body(client)
                await client.close()
            } catch {
                await client.close()
                throw error
            }
            await HarnessGate.shared.release()
        } catch {
            await HarnessGate.shared.release()
            throw error
        }
    }

    /// `withHarness` plus a connected socket, for channel tests.
    static func withHarnessSocket(
        _ body: @Sendable (HarnessServiceClient, Socket) async throws -> Void
    ) async throws {
        try await withHarness { client in
            let socket = try await client.openSocket()
            try await body(client, socket)
        }
    }

    /// Await the first value delivered to a callback-based subscription.
    static func awaitFirst<T: Sendable>(
        timeout: TimeInterval = 2.0,
        _ register: @Sendable (@escaping @Sendable (T) -> Void) -> Void
    ) async throws -> T {
        let (stream, continuation) = AsyncStream<T>.makeStream()
        register { value in continuation.yield(value) }
        return try await withThrowingTaskGroup(of: T?.self) { group in
            group.addTask {
                for await value in stream { return value }
                return nil
            }
            group.addTask {
                try await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
                return nil
            }
            guard let first = try await group.next(), let value = first else {
                group.cancelAll()
                throw ContractTestError.timeout("No value delivered within \(timeout)s")
            }
            group.cancelAll()
            return value
        }
    }
}

// MARK: - Global harness serialization

/// FIFO async gate serializing every harness-backed test across all suites —
/// scenario registration and reset are global harness state.
actor HarnessGate {
    static let shared = HarnessGate()

    private var locked = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func acquire() async {
        if !locked {
            locked = true
            return
        }
        await withCheckedContinuation { waiters.append($0) }
    }

    func release() {
        if waiters.isEmpty {
            locked = false
        } else {
            let next = waiters.removeFirst()
            next.resume()
        }
    }
}

// MARK: - Subprocess lifecycle

struct HarnessUrls: Sendable {
    let wsUrl: String
    let controlUrl: String
}

/// Boots Prism and the channel-harness service on demand, once per test
/// process, and kills them at exit.
actor TestServers {
    static let shared = TestServers()

    private var prismProcess: Process?
    private var prismStdin: Pipe?
    private var prismStderr: FileHandle?
    private var prismReady = false
    private var prismStarting = false
    private var prismStartError: (any Error)?
    private var harnessProcess: Process?
    private var harnessStdin: Pipe?
    private var harnessStderr: FileHandle?
    private var harnessUrls: HarnessUrls?
    private var harnessStartError: (any Error)?

    // MARK: Prism

    private func httpAnswers(_ urlString: String) async -> Bool {
        guard let url = URL(string: urlString) else { return false }
        var request = URLRequest(url: url)
        request.timeoutInterval = 1
        return (try? await URLSession.shared.data(for: request)) != nil
    }

    private func prismAnswers() async -> Bool {
        await httpAnswers("\(ContractSupport.prismURL)/")
    }

    func ensurePrism() async throws {
        if prismReady { return }
        if let prismStartError { throw prismStartError }
        if prismStarting {
            while prismStarting {
                try await Task.sleep(nanoseconds: 50_000_000)
            }
            if prismReady { return }
            if let prismStartError { throw prismStartError }
            return
        }

        // Set before any await so parallel suites cannot spawn a second
        // Prism on the same port (that race wedged GitHub-hosted macOS).
        prismStarting = true
        defer { prismStarting = false }

        do {
            if await prismAnswers() {
                prismReady = true
                return
            }
            try await startPrismProcess()
            prismReady = true
        } catch {
            prismStartError = error
            throw error
        }
    }

    private func startPrismProcess() async throws {
        let bin = ContractSupport.prismBin
        guard FileManager.default.fileExists(atPath: bin) else {
            throw ContractTestError.prismStartFailed(
                "Prism bin not found at \(bin). Set PRISM_BIN or run 'npm ci' in the package root."
            )
        }

        let process = Process()
        process.executableURL = URL(fileURLWithPath: bin)
        // Static examples keep shape-contract tests deterministic. Prism's
        // dynamic faker can hang or crash on valid nested oneOf schemas
        // (ActivityFeed); JS and Python already run static for that reason.
        process.arguments = [
            "mock", ContractSupport.specPath,
            "--port", ContractSupport.prismPort,
            "--host", "127.0.0.1",
        ]
        // Hold stdin open — the test runner's own stdin may be closed, and
        // an inherited closed stdin can make child processes exit early.
        let stdin = Pipe()
        let stderrURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("archastro-prism-\(ProcessInfo.processInfo.processIdentifier).err")
        FileManager.default.createFile(atPath: stderrURL.path, contents: nil)
        let stderrHandle = try FileHandle(forWritingTo: stderrURL)
        process.standardInput = stdin
        process.standardOutput = FileHandle.nullDevice
        process.standardError = stderrHandle
        try process.run()
        prismProcess = process
        prismStdin = stdin
        prismStderr = stderrHandle
        ProcessReaper.shared.track(process)

        let deadline = Date().addingTimeInterval(30)
        while Date() < deadline {
            if await prismAnswers() {
                return
            }
            if !process.isRunning {
                throw ContractTestError.prismStartFailed(
                    "Prism exited with code \(process.terminationStatus)\n\(readFile(stderrURL))"
                )
            }
            try await Task.sleep(nanoseconds: 300_000_000)
        }
        process.terminate()
        throw ContractTestError.prismStartFailed(
            "Prism did not start on port \(ContractSupport.prismPort) within 30s\n\(readFile(stderrURL))"
        )
    }

    // MARK: Harness

    func ensureHarness() async throws -> HarnessUrls {
        if let harnessUrls { return harnessUrls }
        if let harnessStartError { throw harnessStartError }
        do {
            let urls = try await startHarness()
            harnessUrls = urls
            return urls
        } catch {
            harnessStartError = error
            throw error
        }
    }

    private func startHarness() async throws -> HarnessUrls {
        // CI (or a developer) can pre-start the service and hand us URLs.
        // Skip spawn so we never block the test process on child I/O.
        if let ws = ProcessInfo.processInfo.environment["ARCHASTRO_HARNESS_WS_URL"],
           let control = ProcessInfo.processInfo.environment["ARCHASTRO_HARNESS_CONTROL_URL"],
           !ws.isEmpty, !control.isEmpty
        {
            return HarnessUrls(wsUrl: ws, controlUrl: control)
        }

        let bin = ContractSupport.harnessBin
        guard FileManager.default.fileExists(atPath: bin) else {
            throw ContractTestError.harnessStartFailed(
                "channel-harness bin not found at \(bin). Set ARCHASTRO_HARNESS_BIN or run 'npm ci' in the package root."
            )
        }

        // Bind fixed ports and poll GET /health. FileHandle.availableData and
        // readabilityHandler both block the TestServers actor under
        // `swift test` on GitHub-hosted macOS, so the previous 15s stdout
        // deadline never fired and every contract test stalled with it.
        let wsPort = ProcessInfo.processInfo.environment["ARCHASTRO_HARNESS_WS_PORT"] ?? "18765"
        let controlPort = ProcessInfo.processInfo.environment["ARCHASTRO_HARNESS_CONTROL_PORT"] ?? "18766"
        let urls = HarnessUrls(
            wsUrl: "ws://127.0.0.1:\(wsPort)/socket/websocket",
            controlUrl: "http://127.0.0.1:\(controlPort)"
        )

        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = [
            "node", bin, ContractSupport.specPath,
            "--ws-port", wsPort,
            "--control-port", controlPort,
        ]
        process.currentDirectoryURL = ContractSupport.packageRoot
        process.environment = ProcessInfo.processInfo.environment
        // Hold stdin open — the harness exits on stdin_close, and
        // `swift test` in GitHub Actions starts with stdin already closed.
        let stdin = Pipe()
        let stderrURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("archastro-harness-\(ProcessInfo.processInfo.processIdentifier).err")
        FileManager.default.createFile(atPath: stderrURL.path, contents: nil)
        let stderrHandle = try FileHandle(forWritingTo: stderrURL)
        process.standardInput = stdin
        process.standardOutput = FileHandle.nullDevice
        process.standardError = stderrHandle
        try process.run()
        harnessProcess = process
        harnessStdin = stdin
        harnessStderr = stderrHandle
        ProcessReaper.shared.track(process)

        let deadline = Date().addingTimeInterval(30)
        while Date() < deadline {
            if !process.isRunning {
                throw ContractTestError.harnessStartFailed(
                    "harness service exited with code \(process.terminationStatus) before /health answered\n\(readFile(stderrURL))"
                )
            }
            if await httpAnswers("\(urls.controlUrl)/health") {
                return urls
            }
            try await Task.sleep(nanoseconds: 100_000_000)
        }
        process.terminate()
        throw ContractTestError.harnessStartFailed(
            "harness service did not answer \(urls.controlUrl)/health within 30s\n\(readFile(stderrURL))"
        )
    }

    private func readFile(_ url: URL) -> String {
        (try? String(contentsOf: url, encoding: .utf8)) ?? ""
    }
}

/// Kills tracked subprocesses when the test process exits.
final class ProcessReaper: @unchecked Sendable {
    static let shared = ProcessReaper()

    private let processes = Locked<[Process]>([])

    private init() {
        atexit {
            ProcessReaper.shared.killAll()
        }
    }

    func track(_ process: Process) {
        processes.withLock { $0.append(process) }
    }

    func killAll() {
        let tracked = processes.withLock { procs -> [Process] in
            let copy = procs
            procs = []
            return copy
        }
        for process in tracked where process.isRunning {
            process.terminate()
        }
    }
}

#endif
