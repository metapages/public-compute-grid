/**
 * `worker run --auto-update`: the worker replaces itself with the latest
 * release, keeping its configuration and its worker id, without leaving the
 * queue without a worker.
 *
 * A worker never hands over running jobs: only the worker that ran a job
 * knows when it finished and uploads its outputs. So an upgrade pauses new
 * jobs as soon as it begins, and completes only once the running ones are
 * done.
 *
 * In docker (the usual install):
 *
 *   predecessor (this version)                successor (new version)
 *   ─────────────────────────                 ───────────────────────
 *   newer release found, server gate allows
 *   pull the image
 *   "draining": stops claiming new jobs
 *   create a container that is this one
 *   with only the image changed           ──▶ starts in "standby": connects,
 *                                             registers with the same worker
 *                                             id, claims nothing
 *   sees it in /q/<queue>/status
 *   (same id, new version, standby)
 *   waits for its running jobs to finish
 *   (and upload their outputs)
 *   "retiring" (only ever reported with
 *   no jobs left)                         ──▶ sees "retiring" in the status
 *                                             stops + removes the predecessor
 *                                             takes its container name
 *                                             claims jobs
 *
 * Any failure before the successor takes over (no image, it never registers,
 * it dies, jobs outlast --max-job-duration, it never stops us) removes the
 * successor; this worker claims jobs again and retries later. The successor
 * is what stops the predecessor, so a failed upgrade never leaves the queue
 * with no worker.
 *
 * A `deno compile`d binary re-execs in place instead: download, check the new
 * binary runs, pause and wait for jobs, swap the file, execv. Same pid
 * throughout.
 *
 * Fly and GCP deployments are recreated from their config, which is being
 * deprecated: they are not handled here (the container they run in is not
 * theirs to recreate through a docker socket anyway).
 */
import type { Container, ContainerInspectInfo } from "dockerode";
import {
  compareReleaseVersions,
  isWorkerUpgradeAllowed,
  type WorkerStatusResponse,
} from "@metapages/compute-queues-shared";
import humanizeDuration from "humanize-duration";

import { config } from "/@/config.ts";
import { docker } from "/@/queue/dockerClient.ts";
import type { DockerJobQueue } from "/@/queue/DockerJobQueue.ts";

import mod from "../../mod.json" with { type: "json" };
import { predecessorStateFrom, retryDelay, successorContainerName, takeoverDecision } from "./handover.ts";
import { type HandoverDeps, handOverToSuccessor, waitForRunningJobs } from "./handoverFlow.ts";
import { binaryDownloadUrl, getLatestReleaseVersion, getQueueWorkers, getWorkerVersionGate } from "./releases.ts";
import { execInPlace, inspectOwnContainer, isCompiledBinary, isInContainer } from "./self.ts";
import { HANDOVER_FROM_ENV, HANDOVER_ID_ENV, successorImage, successorSpec } from "./successor.ts";

const VERSION: string = mod.version;

const POLL_INTERVAL = 3_000;

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const log = (message: string) => console.log(`🔁 auto-update: ${message}`);

const isStandalone = () => Deno.env.get("METAPAGE_IO_WORKER_RUN_STANDALONE") === "true";

type Method = "docker" | "binary";

/** How this worker can replace itself, or why it cannot. */
const detectMethod = async (): Promise<{ method?: Method; reason?: string }> => {
  if (config.mode === "local") {
    return { reason: "not in local mode (the worker is also the API server there)" };
  }
  if (isInContainer()) {
    if (await inspectOwnContainer()) {
      return { method: "docker" };
    }
    return {
      reason: "this container is not visible through /var/run/docker.sock " +
        "(mount the host's docker socket; the standalone image's own dockerd cannot see it)",
    };
  }
  if (isCompiledBinary()) {
    return { method: "binary" };
  }
  return { reason: "running from source (deno run): update the checkout instead" };
};

export interface AutoUpdateArgs {
  queue: DockerJobQueue;
  workerId: string;
  server: string;
  queueId: string;
  intervalMs: number;
  maxJobDurationMs: number;
}

/** Start checking for new releases every `intervalMs`; upgrade when one is allowed. */
export const startAutoUpdate = async (args: AutoUpdateArgs) => {
  const { method, reason } = await detectMethod();
  if (!method) {
    log(`disabled: ${reason}`);
    return;
  }
  log(`enabled (${method}), checking every ${humanizeDuration(args.intervalMs)}, current version ${VERSION}`);

  let failures = 0;
  const refusedLogged = new Set<string>();
  const check = async () => {
    let next = args.intervalMs;
    try {
      // a successor still waiting to take over does not upgrade itself
      if (!args.queue.handover) {
        await checkOnce(args, method, refusedLogged);
      }
      failures = 0;
    } catch (err) {
      failures++;
      next = retryDelay(args.intervalMs, failures);
      log(`💥 upgrade failed, still running ${VERSION}, next try in ${humanizeDuration(next)}: ${err}`);
    }
    setTimeout(check, next);
  };
  // Soon after start, not immediately: let the worker connect first.
  setTimeout(check, Math.min(args.intervalMs, 60_000));
};

const handoverDeps = (args: AutoUpdateArgs, version: string): HandoverDeps => ({
  queue: args.queue,
  workerId: args.workerId,
  version,
  timing: {
    poll: POLL_INTERVAL,
    registerDeadline: 3 * 60_000,
    // the jobs' own time limit, plus time to upload their outputs
    jobsDeadline: args.maxJobDurationMs + 2 * 60_000,
    takeoverDeadline: 3 * 60_000,
  },
  getWorkers: () => getQueueWorkers(args.server, args.queueId),
  sleep,
  now: () => Date.now(),
  log,
});

const checkOnce = async (args: AutoUpdateArgs, method: Method, refusedLogged: Set<string>) => {
  const latest = await getLatestReleaseVersion();
  if (!latest || compareReleaseVersions(latest, VERSION) <= 0) {
    if (config.debug) {
      log(`up to date (${VERSION}, latest ${latest ?? "unknown"})`);
    }
    return;
  }
  const gate = await getWorkerVersionGate(args.server);
  const { allowed, reason } = isWorkerUpgradeAllowed(gate, VERSION, latest);
  if (!allowed) {
    if (!refusedLogged.has(latest)) {
      refusedLogged.add(latest);
      log(`${latest} is out, not upgrading yet: ${reason}`);
    }
    return;
  }
  log(`upgrading ${VERSION} → ${latest} (${reason})`);
  if (method === "docker") {
    await upgradeContainer(args, latest);
  } else {
    await upgradeBinary(args, latest);
  }
};

const pullImage = async (image: string) => {
  try {
    const stream = await docker.pull(image);
    await new Promise<void>((resolve, reject) =>
      docker.modem.followProgress(stream, (err: Error | null) => (err ? reject(err) : resolve()))
    );
  } catch (err) {
    // A locally built or pre-pulled image is fine too
    try {
      await docker.getImage(image).inspect();
    } catch {
      throw new Error(`cannot pull ${image}: ${err}`);
    }
  }
};

/**
 * Docker log output: raw for a tty container, otherwise frames of an 8 byte
 * header (stream, 0, 0, 0, big-endian length) and the payload.
 */
export const demuxDockerLogs = (buffer: Uint8Array): string => {
  const decoder = new TextDecoder();
  const framed = buffer.length >= 8 && buffer[0] <= 2 && buffer[1] === 0 && buffer[2] === 0 && buffer[3] === 0;
  if (!framed) {
    return decoder.decode(buffer);
  }
  let out = "";
  let offset = 0;
  while (offset + 8 <= buffer.length) {
    const length = new DataView(buffer.buffer, buffer.byteOffset + offset + 4, 4).getUint32(0);
    out += decoder.decode(buffer.subarray(offset + 8, offset + 8 + length));
    offset += 8 + length;
  }
  return out;
};

const containerLogsTail = async (container: Container): Promise<string> => {
  try {
    const logs = await container.logs({ stdout: true, stderr: true, tail: 20 }) as unknown as Uint8Array;
    return demuxDockerLogs(logs);
  } catch {
    return "";
  }
};

const assertRunning = async (container: Container, what: string) => {
  let info: ContainerInspectInfo;
  try {
    info = await container.inspect();
  } catch {
    throw new Error(`the successor container disappeared ${what}`);
  }
  if (!info.State?.Running) {
    throw new Error(
      `the successor exited (code ${info.State?.ExitCode}) ${what}:\n${await containerLogsTail(container)}`,
    );
  }
};

const upgradeContainer = async (args: AutoUpdateArgs, version: string) => {
  const self = await inspectOwnContainer();
  if (!self) {
    throw new Error("cannot inspect this worker's own container");
  }
  const image = successorImage(self.Config.Image, version, isStandalone());
  log(`pulling ${image}`);
  await pullImage(image);

  const currentImage = await docker.getImage(self.Image).inspect().catch(() => undefined);
  const name = successorContainerName(self.Name, version);
  // a leftover from an earlier attempt that did not clean up
  await docker.getContainer(name).remove({ force: true }).catch(() => {});

  const spec = successorSpec({
    current: self,
    currentImage,
    image,
    name,
    env: { [HANDOVER_FROM_ENV]: self.Id, [HANDOVER_ID_ENV]: args.workerId },
  });

  // The upgrade begins: no new jobs from here on. The running ones finish
  // here, while the successor starts up alongside.
  await handOverToSuccessor(handoverDeps(args, version), async () => {
    const container = await docker.createContainer(spec.options);
    try {
      for (const network of spec.extraNetworks) {
        await docker.getNetwork(network.name).connect({ Container: container.id, EndpointConfig: network.config });
      }
    } catch (err) {
      // not handed back yet, so nobody else would remove it
      await container.remove({ force: true }).catch(() => {});
      throw err;
    }
    return {
      name: `${name} (${container.id.slice(0, 12)})`,
      start: async () => {
        await container.start();
      },
      assertRunning: (when) => assertRunning(container, when),
      logsTail: () => containerLogsTail(container),
      remove: () => container.remove({ force: true }),
    };
  });
};

const upgradeBinary = async (args: AutoUpdateArgs, version: string) => {
  if (Deno.build.os !== "linux" || Deno.build.arch !== "x86_64") {
    throw new Error(`releases only publish a linux x86_64 binary, this is ${Deno.build.os} ${Deno.build.arch}`);
  }
  const exe = Deno.execPath();
  const download = `${exe}.${version}`;
  const previous = `${exe}.previous`;

  const url = binaryDownloadUrl(version);
  log(`downloading ${url}`);
  const response = await fetch(url, { signal: AbortSignal.timeout(10 * 60_000) });
  if (!response.ok || !response.body) {
    await response.body?.cancel();
    throw new Error(`${url} answered ${response.status}`);
  }
  // Next to the running binary: proves the directory is writable, and makes
  // the final rename atomic.
  const file = await Deno.open(download, { create: true, write: true, truncate: true, mode: 0o755 });
  await response.body.pipeTo(file.writable);
  await Deno.chmod(download, 0o755);

  const { stdout, success } = await new Deno.Command(download, { args: ["version"], stdout: "piped", stderr: "null" })
    .output();
  // the last line: module side effects may log before the command runs
  const reported = new TextDecoder().decode(stdout).trim().split("\n").at(-1)?.trim() ?? "";
  if (!success || reported !== version) {
    await Deno.remove(download).catch(() => {});
    throw new Error(`the downloaded binary reports version "${reported}", expected ${version}`);
  }

  // No running jobs across the exec: pause new ones, let these finish.
  args.queue.enterHandover("draining");
  try {
    await waitForRunningJobs(handoverDeps(args, version));
  } catch (err) {
    await Deno.remove(download).catch(() => {});
    await args.queue.exitHandover();
    throw err;
  }
  args.queue.enterHandover("retiring");
  try {
    await Deno.rename(exe, previous);
    await Deno.rename(download, exe);
  } catch (err) {
    await args.queue.exitHandover();
    throw new Error(`cannot replace ${exe}: ${err}`);
  }
  Deno.env.set(HANDOVER_ID_ENV, args.workerId);
  log(`re-exec ${exe} ${Deno.args.join(" ")} as ${version}`);
  const err = execInPlace(exe, Deno.args);
  // Only reached when the exec failed: put the old binary back.
  await Deno.rename(previous, exe).catch(() => {});
  await args.queue.exitHandover();
  throw err;
};

/**
 * The successor's half of the handover, when started with HANDOVER_FROM_ENV:
 * true if the predecessor is running and this worker should start in standby.
 */
export const shouldStartInStandby = async (): Promise<boolean> => {
  const predecessorId = Deno.env.get(HANDOVER_FROM_ENV);
  if (!predecessorId || config.mode === "local") {
    return false;
  }
  try {
    const predecessor = await docker.getContainer(predecessorId).inspect();
    // The env stays in this container's config: a later restart finds the
    // predecessor long gone and starts normally.
    return !!predecessor.State?.Running;
  } catch {
    return false;
  }
};

/**
 * In standby, wait for the predecessor to say it is retiring (or to die), then
 * stop and remove it, take its container name, and start claiming jobs.
 */
export const takeOverFromPredecessor = async (
  args: { queue: DockerJobQueue; workerId: string; server: string; queueId: string },
) => {
  const predecessorId = Deno.env.get(HANDOVER_FROM_ENV)!;
  const predecessor = docker.getContainer(predecessorId);
  log(`standby: waiting for ${predecessorId.slice(0, 12)} to stop claiming jobs`);
  while (true) {
    const inspected = await predecessor.inspect().then(
      (info) => ({ ok: true as const, info }),
      (error) => ({ ok: false as const, error }),
    );
    const state = predecessorStateFrom(inspected);
    const info = inspected.ok ? inspected.info : undefined;
    let workers: WorkerStatusResponse[] | null = null;
    if (state === "running") {
      try {
        workers = await getQueueWorkers(args.server, args.queueId);
      } catch (err) {
        if (config.debug) {
          log(`cannot read the queue status: ${err}`);
        }
      }
    }
    const ready = takeoverDecision(state, workers, args.workerId) === "take-over";
    if (ready) {
      if (info) {
        log(`stopping predecessor ${info.Name.replace(/^\//, "")} (${predecessorId.slice(0, 12)})`);
        await predecessor.stop({ t: 30 }).catch(() => {});
        await predecessor.remove({ force: true }).catch(() => {});
        const self = await inspectOwnContainer();
        const name = info.Name.replace(/^\//, "");
        if (self && name) {
          await docker.getContainer(self.Id).rename({ name }).then(
            () => log(`renamed to ${name}`),
            (err) => log(`could not take the name ${name}: ${err}`),
          );
        }
      } else {
        log(`predecessor ${predecessorId.slice(0, 12)} is gone, taking over`);
      }
      await args.queue.exitHandover();
      log(`✅ now running ${VERSION}`);
      return;
    }
    await sleep(POLL_INTERVAL);
  }
};
