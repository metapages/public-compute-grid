import { assertEquals, assertRejects, assertStringIncludes } from "std/assert";

import mod from "../mod.json" with { type: "json" };
import { IGNORE_MARKER, isExcludedPath, processRepo, syncPins } from "/@/version-pins.ts";

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

Deno.test("an inline marker exempts only its own line, not the pin below it", () => {
  const text = [`${IMAGE}0.1.0 # ${IGNORE_MARKER}`, `${IMAGE}0.2.0`].join("\n");
  const result = syncPins("README.md", text, "1.2.3");
  assertEquals(result.stale, [{ path: "README.md", line: 2, found: "0.2.0" }]);
  assertEquals(result.text, [`${IMAGE}0.1.0 # ${IGNORE_MARKER}`, `${IMAGE}1.2.3`].join("\n"));
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

const decoder = new TextDecoder();

const run = async (cmd: string, args: string[], cwd: string) => {
  const { code, stdout, stderr } = await new Deno.Command(cmd, { args, cwd, stdout: "piped", stderr: "piped" })
    .output();
  return { code, stdout: decoder.decode(stdout), stderr: decoder.decode(stderr) };
};

/** A temp git repo holding `files`, all committed */
const tempRepo = async (files: Record<string, string | Uint8Array>): Promise<string> => {
  const root = await Deno.makeTempDir({ prefix: "version-pins-" });
  for (const [path, content] of Object.entries(files)) {
    const full = `${root}/${path}`;
    await Deno.mkdir(full.slice(0, full.lastIndexOf("/")), { recursive: true });
    await (typeof content === "string" ? Deno.writeTextFile(full, content) : Deno.writeFile(full, content));
  }
  for (
    const args of [["init", "-q"], ["add", "-A"], ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "x"]]
  ) {
    const { code, stderr } = await run("git", args, root);
    assertEquals(code, 0, stderr);
  }
  return root;
};

const fixture = () => ({
  "README.md": `docker run ${IMAGE}0.1.0 run q\n`,
  "docs/a b.md": `image: ${IMAGE}0.1.0-standalone\n`,
  "current.md": `${IMAGE}1.2.3\n`,
  // A NUL byte makes it binary, so its pin-looking bytes are skipped
  "logo.png": new Uint8Array([0, ...new TextEncoder().encode(`${IMAGE}0.1.0`)]),
  "deleted.md": `${IMAGE}0.1.0\n`,
});

Deno.test("processRepo: check finds stale pins and writes nothing; binary and deleted files are skipped", async () => {
  const root = await tempRepo(fixture());
  try {
    await Deno.remove(`${root}/deleted.md`);
    const before = await Deno.readTextFile(`${root}/README.md`);
    const stale = await processRepo(root, "1.2.3", false);
    assertEquals(stale.map((p) => `${p.path}:${p.line}:${p.found}`).sort(), [
      "README.md:1:0.1.0",
      "docs/a b.md:1:0.1.0",
    ]);
    assertEquals(await Deno.readTextFile(`${root}/README.md`), before);
  } finally {
    await Deno.remove(root, { recursive: true });
  }
});

Deno.test("processRepo: sync rewrites the stale files, and a second pass finds nothing", async () => {
  const root = await tempRepo(fixture());
  try {
    assertEquals((await processRepo(root, "1.2.3", true)).length, 3);
    assertEquals(await Deno.readTextFile(`${root}/README.md`), `docker run ${IMAGE}1.2.3 run q\n`);
    assertEquals(await Deno.readTextFile(`${root}/docs/a b.md`), `image: ${IMAGE}1.2.3-standalone\n`);
    assertEquals(await processRepo(root, "1.2.3", false), []);
  } finally {
    await Deno.remove(root, { recursive: true });
  }
});

Deno.test("processRepo: fails, instead of passing, when git can't list the files", async () => {
  const notARepo = await Deno.makeTempDir({ prefix: "version-pins-norepo-" });
  try {
    await assertRejects(() => processRepo(notARepo, "1.2.3", false), Error, "git ls-files");
  } finally {
    await Deno.remove(notARepo, { recursive: true });
  }
});

const SCRIPT = new URL("./version-pins.ts", import.meta.url).pathname;
const cli = (args: string[], cwd: string) => run(Deno.execPath(), ["run", "--allow-all", SCRIPT, ...args], cwd);

Deno.test("cli: check exits non-zero with file:line on drift, and when git fails", async () => {
  const root = await tempRepo({ "README.md": `${IMAGE}0.0.1\n` });
  const notARepo = await Deno.makeTempDir({ prefix: "version-pins-norepo-" });
  try {
    const drift = await cli(["check", root], root);
    assertEquals(drift.code, 1);
    assertStringIncludes(drift.stderr, `README.md:1: 0.0.1 != ${mod.version}`);

    const noGit = await cli(["check", notARepo], notARepo);
    assertEquals(noGit.code, 1);
    assertStringIncludes(noGit.stderr, "git rev-parse --show-toplevel failed");
  } finally {
    await Deno.remove(root, { recursive: true });
    await Deno.remove(notARepo, { recursive: true });
  }
});

Deno.test("cli: sync --files-only prints exactly the rewritten paths on stdout, for just deploy", async () => {
  const root = await tempRepo({
    "README.md": `${IMAGE}0.0.1\n${IMAGE}0.0.1\n`,
    "docs/a b.md": `${IMAGE}0.0.1\n`,
    "current.md": `${IMAGE}${mod.version}\n`,
  });
  try {
    const { code, stdout, stderr } = await cli(["sync", "--files-only", root], root);
    assertEquals(code, 0, stderr);
    assertEquals(stdout.trim().split("\n").sort(), ["README.md", "docs/a b.md"]);
    assertStringIncludes(stderr, `README.md:2: 0.0.1 -> ${mod.version}`);
    assertEquals((await cli(["check", root], root)).code, 0);
  } finally {
    await Deno.remove(root, { recursive: true });
  }
});
