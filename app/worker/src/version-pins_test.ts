import { assertEquals } from "std/assert";

import { IGNORE_MARKER, isExcludedPath, syncPins } from "/@/version-pins.ts";

// Built up so this file holds no literal pins for the repo-wide check to flag
const IMAGE = ["metapage/metaframe-docker-worker", ""].join(":");

Deno.test("syncPins rewrites semver pins and keeps the suffix", () => {
  const text = [
    `docker run ${IMAGE}0.39.0 run public1`,
    `  image = '${IMAGE}0.54.62-standalone'`,
    `already ${IMAGE}1.2.3`,
  ].join("\n");
  const result = syncPins("docs/x.md", text, "1.2.3");
  assertEquals(
    result.text,
    [
      `docker run ${IMAGE}1.2.3 run public1`,
      `  image = '${IMAGE}1.2.3-standalone'`,
      `already ${IMAGE}1.2.3`,
    ].join("\n"),
  );
  assertEquals(result.stale, [
    { path: "docs/x.md", line: 1, found: "0.39.0" },
    { path: "docs/x.md", line: 2, found: "0.54.62" },
  ]);
});

Deno.test("syncPins leaves non-semver tags alone", () => {
  const text = [`${IMAGE}latest`, `${IMAGE}local`, `${IMAGE}$VERSION-standalone`].join("\n");
  const result = syncPins("justfile", text, "1.2.3");
  assertEquals(result.text, text);
  assertEquals(result.stale, []);
});

Deno.test("syncPins rewrites the llms.txt Version lines, only in llms.txt", () => {
  const text = ["> Version: 0.54.80", "- **Version**: Current 0.54.80"].join("\n");
  const result = syncPins("app/browser/public/llms.txt", text, "1.2.3");
  assertEquals(result.text, ["> Version: 1.2.3", "- **Version**: Current 1.2.3"].join("\n"));
  assertEquals(result.stale.map((p) => p.line), [1, 2]);

  assertEquals(syncPins("docs/other.md", text, "1.2.3").stale, []);
});

Deno.test("syncPins skips lines carrying the ignore marker, or below it", () => {
  const text = [
    `${IMAGE}0.1.0 # ${IGNORE_MARKER}`,
    `# ${IGNORE_MARKER}`,
    `${IMAGE}0.1.0`,
    `${IMAGE}0.1.0`,
  ].join("\n");
  const result = syncPins("README.md", text, "1.2.3");
  assertEquals(result.stale, [{ path: "README.md", line: 4, found: "0.1.0" }]);
  assertEquals(result.text.split("\n")[3], `${IMAGE}1.2.3`);
});

Deno.test("lockfiles, changelogs and test files are excluded", () => {
  for (
    const path of [
      "deno.lock",
      "docs/package-lock.json",
      "pnpm-lock.yaml",
      "CHANGELOG.md",
      "app/x/changelog.txt",
      "app/worker/src/upgrade/successor_test.ts",
      "app/browser/src/thing.test.tsx",
    ]
  ) {
    assertEquals(isExcludedPath(path), true, path);
    assertEquals(syncPins(path, `${IMAGE}0.1.0`, "1.2.3").stale, []);
  }
  assertEquals(isExcludedPath("README.md"), false);
  assertEquals(isExcludedPath("app/worker/src/upgrade/successor.ts"), false);
});
