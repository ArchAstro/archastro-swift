#!/usr/bin/env bash
# Regenerate the Swift SDK + contract tests from the canonical OpenAPI spec.
#
# Usage:
#   ./scripts/regenerate_sdk.sh            # fetch spec from GitHub (main)
#   ./scripts/regenerate_sdk.sh --local <archastro-openapi-checkout>
#
# Env:
#   ARCHASTRO_OPENAPI_REF        git ref to fetch the spec from (default: main)
#   ARCHASTRO_SDK_GENERATOR_BIN  path to the sdk-generator entry point
#                                (default: node_modules/.bin/sdk-generator)

set -euo pipefail
cd "$(dirname "$0")/.."

SPEC_DEST="specs/platform-openapi.json"
CONFIG="scripts/sdk-generator-config.json"

if [[ "${1:-}" == "--local" ]]; then
  SRC="${2:?usage: regenerate_sdk.sh --local <archastro-openapi-checkout>}"
  mkdir -p specs
  cp "$SRC/specs/platform-openapi.json" "$SPEC_DEST"
  echo "Copied spec from $SRC"
else
  REF="${ARCHASTRO_OPENAPI_REF:-main}"
  mkdir -p specs
  curl -fsSL "https://raw.githubusercontent.com/ArchAstro/archastro-openapi/$REF/specs/platform-openapi.json" \
    -o "$SPEC_DEST"
  echo "Fetched spec at ref $REF"
fi

GENERATOR="${ARCHASTRO_SDK_GENERATOR_BIN:-node_modules/.bin/sdk-generator}"

if [[ ! -e "$GENERATOR" ]]; then
  echo "sdk-generator not found at $GENERATOR — run 'npm ci' or set ARCHASTRO_SDK_GENERATOR_BIN" >&2
  exit 1
fi

"$GENERATOR" --spec "$SPEC_DEST" --config "$CONFIG" --lang swift --out .
"$GENERATOR" --spec "$SPEC_DEST" --config "$CONFIG" --lang contract-tests-swift --out .

echo "Done. Build with 'swift build', test with 'swift test'."
