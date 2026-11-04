import { assert, assertEquals, assertStringIncludes } from "std/assert";
import { join } from "std/path";

const STATE_ROOT = "/tmp/pr-daemon-compute-grid";
const JUSTFILE = new URL("../../../justfile", import.meta.url);

const withoutKeepData = () => {
  const inherited = Deno.env.toObject();
  delete inherited.PR_DAEMON_KEEP_DATA;
  return inherited;
};

const run = async (cmd: string, args: string[], cwd: string, env: Record<string, string> = {}) => {
  const { success, stderr } = await new Deno.Command(cmd, {
    args,
    cwd,
    env: { ...withoutKeepData(), ...env },
    clearEnv: true,
    stdout: "null",
    stderr: "piped",
  }).output();
  return { success, stderr: new TextDecoder().decode(stderr) };
};

const mustRun = async (cmd: string, args: string[], cwd: string, env: Record<string, string> = {}) => {
  const { success, stderr } = await run(cmd, args, cwd, env);
  assert(success, `${cmd} ${args.join(" ")}: ${stderr}`);
};

const exists = async (path: string) => {
  try {
    await Deno.stat(path);
    return true;
  } catch (_) {
    return false;
  }
};

/**
 * The real justfile in a throwaway repo on a throwaway branch, so that it derives
 * a state directory no running preview owns, holding what a dead stack leaves.
 */
const withStack = async (check: (args: { repo: string; state: string }) => Promise<void>) => {
  const branch = `test-down-${crypto.randomUUID().slice(0, 8)}`;
  const state = join(STATE_ROOT, branch);
  const repo = await Deno.makeTempDir();
  try {
    await Deno.copyFile(JUSTFILE, join(repo, "justfile"));
    await mustRun("git", ["init", "-q", "-b", branch], repo);
    await mustRun("git", [
      "-c",
      "user.name=test",
      "-c",
      "user.email=test@test",
      "-c",
      "commit.gpgsign=false",
      "commit",
      "-q",
      "--allow-empty",
      "-m",
      "empty",
    ], repo);

    const data = join(state, "worker-data/local");
    await Deno.mkdir(join(data, "f"), { recursive: true });
    await Deno.mkdir(join(data, "docker-builds/downloads"), { recursive: true });
    await Deno.mkdir(join(data, "j/job/outputs"), { recursive: true });
    await Deno.writeTextFile(join(data, "kv"), "jobs");
    await Deno.writeTextFile(join(data, "f/abc"), "output");
    await Deno.writeTextFile(join(data, "f/abc.1.tmp"), "half an upload");
    await Deno.writeTextFile(join(data, "docker-builds/downloads/ctx.tar.gz"), "archive");
    await Deno.writeTextFile(join(data, "docker-builds/downloads/ctx.tar.gz.1.tmp"), "half a download");
    await Deno.writeTextFile(join(data, "j/job/outputs/mine.tmp"), "a job's own file");
    await Deno.writeTextFile(join(state, "worker.log"), "log");
    await Deno.writeTextFile(join(state, "port"), "1");
    // Not a real pid: `down` must get past a stale one
    await Deno.writeTextFile(join(state, "worker.pid"), "999999");

    await check({ repo, state });
  } finally {
    await Deno.remove(repo, { recursive: true });
    await Deno.remove(state, { recursive: true }).catch(() => {});
  }
};

const assertDataKept = async (state: string) => {
  const data = join(state, "worker-data/local");
  assertEquals(await Deno.readTextFile(join(data, "kv")), "jobs");
  assertEquals(await Deno.readTextFile(join(data, "f/abc")), "output");
  assertEquals(await Deno.readTextFile(join(data, "docker-builds/downloads/ctx.tar.gz")), "archive");
  assertEquals(await Deno.readTextFile(join(data, "j/job/outputs/mine.tmp")), "a job's own file");
  assertEquals(await exists(join(data, "f/abc.1.tmp")), false);
  assertEquals(await exists(join(data, "docker-builds/downloads/ctx.tar.gz.1.tmp")), false);
  assertEquals(await Deno.readTextFile(join(state, "worker.log.1")), "log");
};

Deno.test("pr-daemon-down keeps the data with PR_DAEMON_KEEP_DATA", async () => {
  await withStack(async ({ repo, state }) => {
    await mustRun("just", ["pr-daemon-down"], repo, { PR_DAEMON_KEEP_DATA: "1" });
    await assertDataKept(state);
    assertEquals(await exists(join(state, "worker.pid")), false);
    assertEquals(await exists(join(state, "port")), false);
    assertEquals(await exists(join(state, "worker.log")), false);
  });
});

Deno.test("pr-daemon-down deletes everything without PR_DAEMON_KEEP_DATA", async () => {
  await withStack(async ({ repo, state }) => {
    await mustRun("just", ["pr-daemon-down"], repo);
    assertEquals(await exists(state), false);
  });
});

// `up` tidies up a dead stack with `down` before it starts: a plain `down` there
// would delete the data the restart just kept, and the data of any crashed stack.
Deno.test("pr-daemon-up starts the worker on the data of the stack it replaces", async () => {
  await withStack(async ({ repo, state }) => {
    // Everything `up` runs after that `down`, down to a worker that exits at once
    for (const app of ["shared", "browser"]) {
      await Deno.mkdir(join(repo, "app", app), { recursive: true });
      await Deno.writeTextFile(join(repo, "app", app, "justfile"), "build:\n    true\n");
    }
    await Deno.mkdir(join(repo, "app/worker"), { recursive: true });
    const bin = join(repo, "bin");
    await Deno.mkdir(bin);
    const argsFile = join(repo, "deno-args");
    await Deno.writeTextFile(join(bin, "deno"), `#!/usr/bin/env bash\necho "$@" > "${argsFile}"\nexit 1\n`);
    await Deno.chmod(join(bin, "deno"), 0o755);

    const { success, stderr } = await run("just", ["pr-daemon-up"], repo, {
      PATH: `${bin}:${Deno.env.get("PATH")}`,
    });
    assert(!success, "the stub worker exits, so up reports that");
    assertStringIncludes(stderr, "stale stack");

    assertStringIncludes(await Deno.readTextFile(argsFile), `--data-directory=${join(state, "worker-data")}`);
    await assertDataKept(state);
  });
});
