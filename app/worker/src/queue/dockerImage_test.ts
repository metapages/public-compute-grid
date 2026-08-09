import { assertEquals, assertThrows } from "std/assert";

import { formatDockerProgressEvent, parseGithubContextUrl } from "./dockerImage.ts";

Deno.test("parseGithubContextUrl: honours a pinned commit", () => {
  const { owner, repo, ref } = parseGithubContextUrl(
    "https://github.com/lenhanpham/OpenThermo/commit/a80273ca1417f0cf29eaf1cee3c1eb5d79bfc4df",
  );
  assertEquals(owner, "lenhanpham");
  assertEquals(repo, "OpenThermo");
  // Regression: this used to read a capture group that does not exist, so every
  // pinned URL resolved to "main" and quietly built the default branch.
  assertEquals(ref, "a80273ca1417f0cf29eaf1cee3c1eb5d79bfc4df");
});

Deno.test("parseGithubContextUrl: honours a branch or tag", () => {
  assertEquals(parseGithubContextUrl("https://github.com/me/tool/tree/v1.2.3").ref, "v1.2.3");
  assertEquals(parseGithubContextUrl("https://github.com/me/tool/tree/release-2").ref, "release-2");
});

Deno.test("parseGithubContextUrl: defaults to main only when no ref is given", () => {
  assertEquals(parseGithubContextUrl("https://github.com/me/tool").ref, "main");
});

Deno.test("parseGithubContextUrl: strips .git and carries a token", () => {
  const parsed = parseGithubContextUrl("https://user:tok@github.com/me/tool.git/tree/dev");
  assertEquals(parsed.repo, "tool");
  assertEquals(parsed.ref, "dev");
  assertEquals(parsed.userPat, "user:tok");
});

Deno.test("parseGithubContextUrl: rejects a non-GitHub URL", () => {
  assertThrows(() => parseGithubContextUrl("https://example.com/not/github"));
});

Deno.test("formatDockerProgressEvent: renders progress objects the way the CLI does", () => {
  // Template-stringifying these yielded "[object Object]" in the build log.
  assertEquals(
    formatDockerProgressEvent({ id: "abc123", status: "Downloading", progress: "[===>] 2MB/8MB" }),
    "abc123: Downloading [===>] 2MB/8MB",
  );
  assertEquals(formatDockerProgressEvent({ status: "Pulling from library/alpine" }), "Pulling from library/alpine");
  assertEquals(formatDockerProgressEvent({ error: "denied" }), "💥 denied");
  assertEquals(formatDockerProgressEvent({ stream: "Step 1/2\n" }), "Step 1/2");
  assertEquals(formatDockerProgressEvent("already a string"), "already a string");
});
