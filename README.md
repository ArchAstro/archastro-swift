# archastro-swift

Swift SDK for the ArchAstro Platform API — generated from the canonical
OpenAPI spec by [`@archastro/sdk-generator`](https://github.com/ArchAstro/archastro-openapi),
with a hand-maintained async runtime. The Swift sibling of
[`archastro-js`](https://github.com/ArchAstro/archastro-js) and
[`archastro-python`](https://github.com/ArchAstro/archastro-python).

Requires Swift 6 / Xcode 16+. Platforms: macOS 13+, iOS 16+, tvOS 16+, watchOS 9+.

## Installation

Swift Package Manager — add the package to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/ArchAstro/archastro-swift.git", from: "0.1.0")
]
```

and depend on the `ArchAstroPlatform` product:

```swift
.target(name: "MyApp", dependencies: [
    .product(name: "ArchAstroPlatform", package: "archastro-swift")
])
```

In Xcode: File → Add Package Dependencies… → paste the repository URL.

## Usage

```swift
import ArchAstroPlatform

// Server-side (secret key)
let client = PlatformClient.withSecretKey("sk_…")

// App-side (publishable key + login)
let client = try await PlatformClient.withCredentials(
    apiKey: "pk_…", email: "dev@example.com", password: "…"
)

// Resources — client.v1.… or the default-version aliases
let agents = try await client.agents.list()
let agent = try await client.agents.create(
    input: AgentCreateInput(name: "support-bot")
)

// SSE streaming
for try await event in client.ai.chat.completions.stream(input: input) {
    print(event.event, event.data)
}

// Realtime channels (Phoenix)
let socket = try await client.openSocket()
let chat = try await ApiChatChannel.joinTeamThread(
    socket: socket, teamId: teamId, threadId: threadId
)
chat.onMessageAdded { payload in print(payload) }
let reply = try await chat.apiChatPostMessage(
    payload: ApiChatPostMessageInput(content: "hello")
)
```

Errors surface as `ApiError` (status, errorCode, message, body). Channel
join failures throw `ChannelError`; push replies return a
`ChannelReply(status:response:)` envelope.

## Project structure

- `Sources/ArchAstroPlatform/Generated/` — **generated, do not edit.**
  Every file carries a `Content hash` header. Models (`Types/`), resources
  (`V1/`), channels (`Channels/`), `Client.swift`, `Auth.swift`.
- `Sources/ArchAstroPlatform/Runtime/` — hand-maintained runtime:
  `HttpClient` (auth headers, one-shot 401 refresh, `ApiError`, SSE),
  `JSONValue`, and the Phoenix channel client (`Socket`, `Channel`).
- `Tests/ArchAstroPlatformTests/` — hand-written runtime unit tests.
- `Tests/ArchAstroPlatformContractTests/` — **generated** contract tests
  (`V1/`, `Channels/`, `Streams/`) plus the hand-maintained `Support/`
  module (Prism/harness lifecycle, `HarnessServiceClient`).

## Regenerating the SDK

```bash
npm ci                      # generator + Prism + channel-harness
./scripts/regenerate_sdk.sh                 # spec from GitHub main
./scripts/regenerate_sdk.sh --local ../archastro-openapi   # local checkout
```

Config lives in `scripts/sdk-generator-config.json`. Env knobs:
`ARCHASTRO_OPENAPI_REF`, `ARCHASTRO_SDK_GENERATOR_BIN`.

## Testing

```bash
swift test --filter ArchAstroPlatformTests        # runtime unit tests
swift test                                        # + REST contract tests (Prism)
ARCHASTRO_RUN_CHANNEL_CONTRACT_TESTS=1 swift test # + channel/stream tests (harness)
```

Contract tests spawn Prism (`node_modules/.bin/prism`) against
`specs/platform-openapi.json` and — when the opt-in env var is set — the
`@archastro/channel-harness` service, exactly like the TypeScript and
Python SDK suites. Overrides: `PRISM_PORT`, `PRISM_BIN`,
`OPENAPI_SPEC_PATH`, `ARCHASTRO_HARNESS_BIN`.

## Releasing

Consumers resolve versions from semver git tags — cut a release with:

```bash
git tag 0.1.0 && git push origin 0.1.0
```

For listing on the [Swift Package Index](https://swiftpackageindex.com),
submit the repository URL once via a PR to
[SwiftPackageIndex/PackageList](https://github.com/SwiftPackageIndex/PackageList);
`.spi.yml` configures its documentation build.

## License

MIT — see [LICENSE](LICENSE). Every source file carries the copyright
header (`scripts/check_headers.sh` enforces it).
