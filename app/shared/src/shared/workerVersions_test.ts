import { assertEquals } from "std/assert";

import {
  compareReleaseVersions,
  isWorkerUpgradeAllowed,
  parseReleaseVersion,
  type WorkerVersionGate,
} from "@shared/workerVersions.ts";

Deno.test("parseReleaseVersion: release versions only", () => {
  assertEquals(parseReleaseVersion("0.54.87"), [0, 54, 87]);
  assertEquals(parseReleaseVersion("v1.2.3"), [1, 2, 3]);
  assertEquals(parseReleaseVersion("1.2.3-rc.1"), null);
  assertEquals(parseReleaseVersion("latest"), null);
  assertEquals(parseReleaseVersion("0.54.87-standalone"), null);
});

Deno.test("compareReleaseVersions: numeric, not lexical", () => {
  assertEquals(compareReleaseVersions("0.54.10", "0.54.9") > 0, true);
  assertEquals(compareReleaseVersions("0.55.0", "0.54.99") > 0, true);
  assertEquals(compareReleaseVersions("1.0.0", "1.0.0"), 0);
  assertEquals(compareReleaseVersions("0.1.0", "1.0.0") < 0, true);
});

const gate: WorkerVersionGate = { server: "0.54.90", maximum: "0.54.90" };

Deno.test("isWorkerUpgradeAllowed: forward, inside the gate", () => {
  assertEquals(isWorkerUpgradeAllowed(gate, "0.54.87", "0.54.90").allowed, true);
  assertEquals(isWorkerUpgradeAllowed(gate, "0.54.87", "0.54.88").allowed, true);
});

Deno.test("isWorkerUpgradeAllowed: never backwards or sideways", () => {
  assertEquals(isWorkerUpgradeAllowed(gate, "0.54.90", "0.54.90").allowed, false);
  assertEquals(isWorkerUpgradeAllowed(gate, "0.54.90", "0.54.89").allowed, false);
});

Deno.test("isWorkerUpgradeAllowed: the server goes first", () => {
  assertEquals(isWorkerUpgradeAllowed(gate, "0.54.87", "0.54.91").allowed, false);
  // a server that does not publish a gate is too old to upgrade against
  assertEquals(isWorkerUpgradeAllowed(null, "0.54.87", "0.54.88").allowed, false);
});

Deno.test("isWorkerUpgradeAllowed: minimum and pre-releases", () => {
  const withMinimum: WorkerVersionGate = { ...gate, minimum: "0.54.89" };
  assertEquals(isWorkerUpgradeAllowed(withMinimum, "0.54.80", "0.54.88").allowed, false);
  assertEquals(isWorkerUpgradeAllowed(withMinimum, "0.54.80", "0.54.89").allowed, true);
  assertEquals(isWorkerUpgradeAllowed(gate, "0.54.87", "0.54.90-rc.1").allowed, false);
});
