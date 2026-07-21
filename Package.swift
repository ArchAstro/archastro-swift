// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "archastro-swift",
    platforms: [
        .macOS(.v13),
        .iOS(.v16),
        .tvOS(.v16),
        .watchOS(.v9),
    ],
    products: [
        .library(name: "ArchAstroPlatform", targets: ["ArchAstroPlatform"])
    ],
    targets: [
        .target(
            name: "ArchAstroPlatform",
            path: "Sources/ArchAstroPlatform"
        ),
        .testTarget(
            name: "ArchAstroPlatformTests",
            dependencies: ["ArchAstroPlatform"],
            path: "Tests/ArchAstroPlatformTests"
        ),
        .testTarget(
            name: "ArchAstroPlatformContractTests",
            dependencies: ["ArchAstroPlatform"],
            path: "Tests/ArchAstroPlatformContractTests"
        ),
    ]
)
