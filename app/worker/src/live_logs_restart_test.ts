/**
 * #360: restart a local-mode worker (which is also the server) in the middle of
 * a job. The logs streamed before the restart must be in the finished job's
 * logs, each line once.
 *
 * A real worker process, killed with SIGKILL (no chance to flush anything on
 * the way out) and started again on the same data directory and worker id, the
 * way an --auto-update successor or a crash-and-restart does. The job builds an
 * image, so it has build logs: nothing replays those after a restart, they only
 * survive if the server kept them. Its run logs are replayed by docker when the
 * new process reattaches to the still-running container, so they must not end
 * up in the result twice.
 *
 * Needs docker; skipped without it.
 */
import { assert, assertEquals } from "std/assert";
import { join } from "std/path";

const WORKER_DIR = new URL("..", import.meta.url).pathname;
// A line every 150ms, so lines are being written while the new process
// reattaches: that is where a replay can overlap the live output
const RUN_LINES = 80;

const dockerAvailable = (() => {
  try {
    return new Deno.Command("docker", { args: ["info"], stdout: "null", stderr: "null" }).outputSync().success;
  } catch (_) {
    return false;
  }
})();

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const freePort = () => {
  const listener = Deno.listen({ port: 0, hostname: "127.0.0.1" });
  const { port } = listener.addr as Deno.NetAddr;
  listener.close();
  return port;
};

const until = async <T>(what: string, timeoutMs: number, check: () => Promise<T | undefined>): Promise<T> => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const value = await check();
      if (value !== undefined) {
        return value;
      }
    } catch (_) {
      // not up yet
    }
    await sleep(250);
  }
  throw new Error(`Timed out waiting for ${what}`);
};

type Logs = { data: [string, number, boolean?][]; isFinal: boolean };

Deno.test({
  name: "live logs: a local-mode worker restarted mid-job keeps the logs streamed before the restart",
  ignore: !dockerAvailable,
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const nonce = crypto.randomUUID().slice(0, 8);
    // A test-worker id: a worker starting up kills the running job containers
    // of any other worker id, except these (DockerJobQueue.killOrphanedJobs).
    // And --no-kill-orphans, so this test's workers leave every other
    // worker's containers on the host alone (other previews, a dev stack).
    const workerId = `test-worker-live-logs-${nonce}`;
    const port = freePort();
    const api = `http://localhost:${port}`;
    const dataDirectory = await Deno.makeTempDir({ prefix: "live-logs-restart-" });
    const logDirectory = await Deno.makeTempDir({ prefix: "live-logs-restart-logs-" });

    let worker: Deno.ChildProcess | undefined;
    const startWorker = async (name: string) => {
      worker = new Deno.Command("deno", {
        args: [
          "run",
          "--allow-all",
          "--unstable-kv",
          "--unstable-cron",
          "src/cli.ts",
          "run",
          "--mode=local",
          "--cpus=1",
          "--gpus=0",
          `--port=${port}`,
          `--data-directory=${dataDirectory}`,
          `--id=${workerId}`,
          "--no-kill-orphans",
        ],
        cwd: WORKER_DIR,
        stdout: "piped",
        stderr: "piped",
        stdin: "null",
      }).spawn();
      // Kept for when the test fails: the worker's output, per process
      for (const [stream, suffix] of [[worker.stdout, "out"], [worker.stderr, "err"]] as const) {
        const file = await Deno.open(join(logDirectory, `${name}.${suffix}.log`), { write: true, create: true });
        stream.pipeTo(file.writable).catch(() => {});
      }
      await until(`worker ${name} to answer`, 60_000, async () => {
        const response = await fetch(`${api}/healthz`);
        await response.body?.cancel();
        return response.ok ? true : undefined;
      });
    };
    const getLogs = async (jobId: string, kind: "build" | "run"): Promise<Logs> => {
      const response = await fetch(`${api}/j/${jobId}/${kind}-logs.json`);
      return await response.json();
    };
    const text = (logs: Logs) => logs.data.map((line) => line[0]).join("");

    let passed = false;
    try {
      await startWorker("first");

      const submitted = await fetch(`${api}/q/local/j`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          definition: {
            build: { dockerfile: `FROM alpine:3.18.5\nRUN echo build-marker-${nonce}\n` },
            command: `sh -c 'for i in $(seq 1 ${RUN_LINES}); do echo run-line-$i; sleep 0.15; done'`,
          },
        }),
      });
      const { jobId } = await submitted.json();
      assert(jobId, "the job was not submitted");

      // Let the job get some way into running, then give the spool its flush
      // interval: the kill below allows nothing else.
      await until("the job's first run lines", 180_000, async () => {
        return text(await getLogs(jobId, "run")).includes("run-line-3\n") ? true : undefined;
      });
      await sleep(1500);
      const buildLogsBefore = await getLogs(jobId, "build");
      const buildBefore = text(buildLogsBefore);
      const runBefore = text(await getLogs(jobId, "run"));
      assert(buildBefore.includes(`build-marker-${nonce}`), `no build logs before the restart:\n${buildBefore}`);
      assert(!runBefore.includes(`run-line-${RUN_LINES}\n`), "the job finished before the restart: make it longer");

      const killedAt = Date.now();
      worker!.kill("SIGKILL");
      await worker!.status;
      // Lines are written to disk within a second of arriving (LogSpool's
      // flushInterval): the ones streamed in the last second before a SIGKILL
      // may be lost. Here those are the image push, which runs alongside the job.
      const buildSpooled = text({
        ...buildLogsBefore,
        data: buildLogsBefore.data.filter((line) => line[1] < killedAt - 1000),
      });

      await startWorker("second");

      // The restarted server still has them while the job is live
      assert(
        text(await getLogs(jobId, "build")).includes(`build-marker-${nonce}`),
        "build logs lost by the restart while the job is still running",
      );

      const finalRun = await until("the job to finish", 120_000, async () => {
        const logs = await getLogs(jobId, "run");
        return logs.isFinal ? logs : undefined;
      });
      const finalBuild = await getLogs(jobId, "build");

      const result = await (await fetch(`${api}/j/${jobId}/result.json`)).json();
      assertEquals(result?.data?.finished?.result?.StatusCode, 0, `job failed: ${JSON.stringify(result)}`);

      const buildText = text(finalBuild);
      assert(
        buildText.includes(`build-marker-${nonce}`),
        `build logs from before the restart are missing:\n${buildText}`,
      );
      assert(
        buildText.startsWith(buildSpooled),
        `the build logs before the restart are not intact:\nbefore:\n${buildSpooled}\nfinal:\n${buildText}`,
      );

      const runText = text(finalRun);
      for (let i = 1; i <= RUN_LINES; i++) {
        const count = runText.split(`run-line-${i}\n`).length - 1;
        assertEquals(count, 1, `run-line-${i} is in the run logs ${count} times:\n${runText}`);
      }
      passed = true;
    } finally {
      try {
        worker?.kill("SIGKILL");
        await worker?.status;
      } catch (_) {
        // already gone
      }
      const containers = new TextDecoder().decode(
        new Deno.Command("docker", {
          args: ["ps", "-aq", "--filter", `label=container.mtfm.io/worker=${workerId}`],
          stderr: "null",
        }).outputSync().stdout,
      ).split("\n").filter(Boolean);
      if (containers.length) {
        new Deno.Command("docker", { args: ["rm", "-f", ...containers], stdout: "null", stderr: "null" }).outputSync();
      }
      await Deno.remove(dataDirectory, { recursive: true }).catch(() => {});
      if (passed) {
        await Deno.remove(logDirectory, { recursive: true }).catch(() => {});
      } else {
        console.log(`The workers' output is in ${logDirectory}`);
      }
    }
  },
});
