#!/usr/bin/env node
// Fails on any npm advisory at or above `moderate` in the JS tooling tree,
// except advisories listed in ALLOWED below.
//
// `npm audit` has no way to waive a single advisory: the only knob is
// --audit-level, and raising that to clear one high-severity finding would
// also hide the next real one. So we read the JSON report and decide here.
//
// An allowlist entry is a dated, justified exception, not a mute button. The
// script fails when an entry expires, and fails when an entry no longer
// matches anything — a waiver that outlives its advisory is a waiver nobody
// re-read.

import { execFileSync } from "node:child_process";

const ALLOWED = {
  "GHSA-qxc2-j82w-r537": {
    package: "@faker-js/faker",
    expires: "2026-12-31",
    reason:
      "Arbitrary code execution via faker.helpers.fake on a caller-supplied " +
      "template. Reaches us only as postman-collection's hard-pinned " +
      "@faker-js/faker 5.5.3, under @stoplight/http-spec, under the Prism " +
      "mock server. Not fixable by upgrading: postman-collection 5.3.1 (latest) " +
      "still pins 5.5.3 exactly, and overriding faker forward breaks " +
      "postman-collection/lib/superstring, which breaks Prism and every " +
      "contract test. Not reachable as we run Prism: Tests/ArchAstroPlatformContractTests/Support/ContractSupport.swift " +
      "starts `prism mock <openapi.json>` against a spec we generate, with no " +
      "--dynamic flag, so no Postman collection is parsed and faker generates " +
      "nothing. Prism is a devDependency and is never part of the published Swift package.",
  },
};

function auditReport() {
  const options = {
    encoding: "utf8",
    maxBuffer: 32 * 1024 * 1024,
    stdio: ["ignore", "pipe", "inherit"],
  };
  try {
    return execFileSync("npm", ["audit", "--json"], options);
  } catch (error) {
    // npm exits non-zero whenever it finds anything at all. That is the normal
    // path here, and the report is still on stdout; only a missing report is
    // a real failure.
    if (typeof error.stdout === "string" && error.stdout.trim() !== "") {
      return error.stdout;
    }
    throw error;
  }
}

const report = JSON.parse(auditReport());

// A registry failure also exits non-zero, with an {"error": ...} body and no
// vulnerabilities map. Without this, an unreachable registry would look like a
// clean tree whose waiver had gone stale — fail-closed, but for the wrong
// reason and with a message that sends you to delete a live waiver.
if (report.error) {
  throw new Error(
    `npm audit could not produce a report: ${report.error.summary ?? JSON.stringify(report.error)}`,
  );
}

const BLOCKING = new Set(["moderate", "high", "critical"]);
const found = new Map();

for (const vuln of Object.values(report.vulnerabilities ?? {})) {
  for (const via of vuln.via ?? []) {
    if (typeof via !== "object" || !via.url) continue;
    if (!BLOCKING.has(via.severity)) continue;
    const id = via.url.split("/").pop();
    if (!found.has(id)) {
      found.set(id, { id, severity: via.severity, package: via.name, title: via.title });
    }
  }
}

const today = new Date().toISOString().slice(0, 10);
const failures = [];

for (const advisory of found.values()) {
  const waiver = ALLOWED[advisory.id];
  if (!waiver) {
    failures.push(
      `${advisory.severity.toUpperCase()} ${advisory.id} (${advisory.package}) — ${advisory.title}`,
    );
  } else if (waiver.expires < today) {
    failures.push(
      `${advisory.id} (${advisory.package}) — waiver expired ${waiver.expires}; re-review it`,
    );
  }
}

for (const [id, waiver] of Object.entries(ALLOWED)) {
  if (!found.has(id)) {
    failures.push(
      `${id} (${waiver.package}) — waiver no longer matches any advisory; delete it from ALLOWED`,
    );
  }
}

if (failures.length > 0) {
  console.error("JS tooling audit failed:\n");
  for (const line of failures) console.error(`  - ${line}`);
  console.error("\nRun `npm audit` for the full report.");
  process.exit(1);
}

const waived = Object.keys(ALLOWED).join(", ");
console.log(
  `JS tooling audit clean at moderate and above${waived ? ` (waived: ${waived})` : ""}.`,
);
