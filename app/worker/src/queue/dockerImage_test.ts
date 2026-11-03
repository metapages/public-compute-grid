import { assertEquals, assertRejects, assertThrows } from "std/assert";
import { join } from "std/path";

import { config } from "../config.ts";
import {
  DockerBuildError,
  downloadContextIntoDirectory,
  formatDockerProgressEvent,
  parseGithubContextUrl,
} from "./dockerImage.ts";

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

const contextArchive = async (dir: string) => {
  await Deno.mkdir(join(dir, "src"));
  await Deno.writeTextFile(join(dir, "src/Dockerfile"), "FROM alpine:3.18.5\n");
  const { success } = await new Deno.Command("tar", {
    args: ["-czf", join(dir, "ctx.tar.gz"), "-C", join(dir, "src"), "Dockerfile"],
  }).output();
  assertEquals(success, true);
  return await Deno.readFile(join(dir, "ctx.tar.gz"));
};

Deno.test("a context download that stops partway is not reused", async () => {
  const dataDirectory = config.dataDirectory;
  config.dataDirectory = await Deno.makeTempDir();
  const archive = await contextArchive(config.dataDirectory);

  let partial: ReadableStreamDefaultController | undefined;
  let requests = 0;
  const server = Deno.serve({ port: 0, hostname: "127.0.0.1", onListen: () => {} }, () => {
    requests++;
    return new Response(
      requests > 1 ? new Blob([archive]) : new ReadableStream({
        start(controller) {
          partial = controller;
          controller.enqueue(archive.slice(0, archive.length / 2));
        },
      }),
    );
  });

  try {
    const context = `http://127.0.0.1:${server.addr.port}/ctx.tar.gz`;
    const downloads = join(config.dataDirectory, "docker-builds/downloads", `127.0.0.1:${server.addr.port}`);
    const destination = join(config.dataDirectory, "build");
    await Deno.mkdir(destination);
    const args = { jobId: "job", context, destination, sender: () => {} };

    const first = downloadContextIntoDirectory(args);
    let names: string[] = [];
    while (names.length === 0 || (await Deno.stat(join(downloads, names[0]))).size === 0) {
      await new Promise((resolve) => setTimeout(resolve, 10));
      try {
        names = (await Array.fromAsync(Deno.readDir(downloads))).map((entry) => entry.name);
      } catch (_) {
        // not created yet
      }
    }
    // What a worker killed at this moment leaves behind: nothing the next job trusts
    assertEquals(names.length, 1);
    assertEquals(names[0].endsWith(".tmp"), true);

    partial!.error(new Error("connection lost"));
    await assertRejects(() => first, DockerBuildError);
    assertEquals(await Array.fromAsync(Deno.readDir(downloads)), []);

    await downloadContextIntoDirectory(args);
    assertEquals(requests, 2);
    assertEquals(await Deno.readTextFile(join(destination, "Dockerfile")), "FROM alpine:3.18.5\n");
    assertEquals((await Array.fromAsync(Deno.readDir(downloads))).map((entry) => entry.name), ["ctx.tar.gz"]);
  } finally {
    // shutdown() waits for a response still open, which is the stalled one if an assert threw
    try {
      partial?.error(new Error("test over"));
    } catch (_) {
      // already errored
    }
    await server.shutdown();
    await Deno.remove(config.dataDirectory, { recursive: true });
    config.dataDirectory = dataDirectory;
  }
});
