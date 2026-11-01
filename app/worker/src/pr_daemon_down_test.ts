import { assert, assertEquals, assertStringIncludes } from "std/assert";
import { join } from "std/path";

const STATE_ROOT = "/tmp/pr-daemon-compute-grid";
const JUSTFILE = new URL("../../../justfile", import.meta.url);

const run = async (cmd: string, args: string[], cwd: string, env: Record<string, string> = {}) => {
  const inherited = Deno.env.toObject();
  delete inherited.PR_DAEMON_KEEP_DATA;
  const { success, stderr } = await new Deno.Command(cmd, {
    args,
    cwd,
    env: { ...inherited, ...env },
    clearEnv: true,
    stdout: "null",
    stderr: "piped",
  }).output();
  assert(success, `${cmd} ${args.join(" ")}: ${new TextDecoder().decode(stderr)}`);
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
 * Runs the real recipe, in a throwaway repo on a throwaway branch so that it
 * derives a state directory no running preview owns. Returns the state directory
 * after `down`, for the test to look at.
 */
const withDown = async (env: Record<string, string>, check: (state: string) => Promise<void>) => {
  const branch = `test-down-${crypto.randomUUID().slice(0, 8)}`;
  const state = join(STATE_ROOT, branch);
  const repo = await Deno.makeTempDir();
  try {
    await Deno.copyFile(JUSTFILE, join(repo, "justfile"));
    await run("git", ["init", "-q", "-b", branch], repo);
    await run("git", [
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

    await Deno.mkdir(join(state, "worker-data/local/f"), { recursive: true });
    await Deno.writeTextFile(join(state, "worker-data/local/kv"), "jobs");
    await Deno.writeTextFile(join(state, "worker-data/local/f/abc"), "output");
    await Deno.writeTextFile(join(state, "worker.log"), "log");
    await Deno.writeTextFile(join(state, "port"), "1");
    // Not a real pid: `down` must get past a stale one
    await Deno.writeTextFile(join(state, "worker.pid"), "999999");

    await run("just", ["pr-daemon-down"], repo, env);
    await check(state);
  } finally {
    await Deno.remove(repo, { recursive: true });
    await Deno.remove(state, { recursive: true }).catch(() => {});
  }
};

Deno.test("pr-daemon-down keeps the data, and only the data, with PR_DAEMON_KEEP_DATA", async () => {
  await withDown({ PR_DAEMON_KEEP_DATA: "1" }, async (state) => {
    assertEquals(await Deno.readTextFile(join(state, "worker-data/local/kv")), "jobs");
    assertEquals(await Deno.readTextFile(join(state, "worker-data/local/f/abc")), "output");
    assertEquals(await exists(join(state, "worker.pid")), false);
    assertEquals(await exists(join(state, "port")), false);
    assertEquals(await exists(join(state, "worker.log")), false);
  });
});

Deno.test("pr-daemon-down deletes everything without PR_DAEMON_KEEP_DATA", async () => {
  await withDown({}, async (state) => {
    assertEquals(await exists(state), false);
  });
});

// `up` tidies up a dead stack with `down` before it starts: a plain `down` there
// would delete the data the restart just kept, and the data of any crashed stack.
Deno.test("pr-daemon-up never deletes the data of the stack it replaces", async () => {
  const justfile = await Deno.readTextFile(JUSTFILE);
  const up = justfile.slice(justfile.indexOf("\npr-daemon-up:"), justfile.indexOf("\npr-daemon-down:"));
  assertStringIncludes(up, "PR_DAEMON_KEEP_DATA=1 just pr-daemon-down");
});
