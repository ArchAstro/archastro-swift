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
    private var prismReady = false
    private var harnessProcess: Process?
    private var harnessStdin: Pipe?
    private var harnessUrls: HarnessUrls?

    // MARK: Prism

    private func prismAnswers() async -> Bool {
        guard let probeURL = URL(string: "\(ContractSupport.prismURL)/") else { return false }
        return (try? await URLSession.shared.data(from: probeURL)) != nil
    }

    func ensurePrism() async throws {
        if prismReady { return }

        // A Prism already listening (a prior run, or started externally)
        // serves the same spec — use it.
        if await prismAnswers() {
            prismReady = true
            return
        }

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
        process.standardInput = stdin
        process.standardOutput = FileHandle.nullDevice
        process.standardError = FileHandle.nullDevice
        try process.run()
        prismProcess = process
        prismStdin = stdin
        ProcessReaper.shared.track(process)

        // Poll until Prism answers.
        let deadline = Date().addingTimeInterval(30)
        while Date() < deadline {
            if await prismAnswers() {
                prismReady = true
                return
            }
            if !process.isRunning {
                throw ContractTestError.prismStartFailed(
                    "Prism exited with code \(process.terminationStatus)"
                )
            }
            try await Task.sleep(nanoseconds: 300_000_000)
        }
        throw ContractTestError.prismStartFailed(
            "Prism did not start on port \(ContractSupport.prismPort) within 30s"
        )
    }

    // MARK: Harness

    func ensureHarness() async throws -> HarnessUrls {
        if let harnessUrls { return harnessUrls }

        let bin = ContractSupport.harnessBin
        guard FileManager.default.fileExists(atPath: bin) else {
            throw ContractTestError.harnessStartFailed(
                "channel-harness bin not found at \(bin). Set ARCHASTRO_HARNESS_BIN or run 'npm ci' in the package root."
            )
        }

        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = ["node", bin, ContractSupport.specPath]
        let stdout = Pipe()
        // The harness exits when its stdin closes — hold a pipe open.
        let stdin = Pipe()
        process.standardInput = stdin
        process.standardOutput = stdout
        process.standardError = FileHandle.nullDevice
        try process.run()
        harnessProcess = process
        harnessStdin = stdin
        ProcessReaper.shared.track(process)

        // The service prints exactly one JSON line with its URLs.
        let firstLine = LineCollector()
        stdout.fileHandleForReading.readabilityHandler = { handle in
            firstLine.append(handle.availableData)
        }

        let line = try await withTimeoutOrError(
            15, ContractTestError.harnessStartFailed("harness service did not report URLs within 15s")
        ) {
            await firstLine.firstLine()
        }
        guard
            let parsed = try? JSONCoding.decoder.decode(JSONValue.self, from: Data(line.utf8)),
            let ws = parsed["wsUrl"]?.stringValue,
            let control = parsed["controlUrl"]?.stringValue
        else {
            throw ContractTestError.harnessStartFailed("Unparseable harness URL line: \(line)")
        }
        let urls = HarnessUrls(wsUrl: ws, controlUrl: control)
        harnessUrls = urls
        return urls
    }
}

/// Accumulates subprocess stdout and hands out the first complete line.
final class LineCollector: @unchecked Sendable {
    private let state = Locked<(buffer: Data, continuations: [CheckedContinuation<String, Never>], line: String?)>(
        (Data(), [], nil)
    )

    func append(_ data: Data) {
        guard !data.isEmpty else { return }
        let resumptions: [(CheckedContinuation<String, Never>, String)] = state.withLock { state in
            if state.line != nil { return [] }
            state.buffer.append(data)
            guard let newline = state.buffer.firstIndex(of: UInt8(ascii: "\n")) else { return [] }
            let lineData = state.buffer[state.buffer.startIndex..<newline]
            let line = String(decoding: lineData, as: UTF8.self)
                .trimmingCharacters(in: .whitespaces)
            state.line = line
            let waiting = state.continuations
            state.continuations = []
            return waiting.map { ($0, line) }
        }
        for (continuation, line) in resumptions {
            continuation.resume(returning: line)
        }
    }

    func firstLine() async -> String {
        if let line = state.withLock({ $0.line }) { return line }
        return await withCheckedContinuation { continuation in
            let ready: String? = state.withLock { state in
                if let line = state.line { return line }
                state.continuations.append(continuation)
                return nil
            }
            if let ready { continuation.resume(returning: ready) }
        }
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
