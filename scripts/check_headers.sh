#!/usr/bin/env bash
# Copyright (c) 2026 ArchAstro Inc. Licensed under the MIT License.
#
# Verify every Swift source file carries the ArchAstro copyright header.
# Package.swift keeps its header on line 2 (swift-tools-version must be
# line 1).

set -euo pipefail
cd "$(dirname "$0")/.."

missing=0
while IFS= read -r file; do
  if ! head -2 "$file" | grep -q "Copyright (c) .* ArchAstro Inc\."; then
    echo "missing header: $file" >&2
    missing=1
  fi
done < <(find Package.swift Sources Tests -name '*.swift' -not -path '*/.build/*')

if [[ "$missing" -ne 0 ]]; then
  echo "Add the copyright header to the files above." >&2
  exit 1
fi
echo "All Swift files carry the copyright header."
