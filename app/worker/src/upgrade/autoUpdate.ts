/**
 * `worker run --auto-update`: the worker replaces itself with the latest
 * release, keeping its configuration and its worker id, without leaving the
 * queue without a worker.
 *
 * In docker (the usual install):
 *
 *   predecessor (this version)                successor (new version)
 *   ─────────────────────────                 ───────────────────────
 *   newer release found, server gate allows
 *   pull image, create a container that is
 *   this one with only the image changed  ──▶ starts in "standby": connects,
 *                                             registers with the same worker
 *                                             id, claims nothing
 *   sees it in /q/<queue>/status
 *   (same id, new version, standby)
 *   "draining": stops claiming jobs, waits
 *   until its running jobs can be handed
 *   over (or have finished, standalone)
 *   "retiring"                            ──▶ sees "retiring" in the status
 *                                             stops + removes the predecessor
 *                                             takes its container name
 *                                             claims jobs: running ones are
 *                                             adopted, their containers kept
 *
 * Any failure before the successor takes over (no image, it never registers,
 * it dies, it never stops us) removes the successor; this worker claims jobs
 * again and backs off. The successor is what stops the predecessor, so a
 * failed upgrade never leaves the queue with no worker.
 *
 * A `deno compile`d binary re-execs in place instead: download, check the new
 * binary runs, drain, swap the file, execv. Same pid throughout.
 *
 * Fly and GCP deployments are recreated from their config, which is being
 * deprecated: they are not handled here (the container they run in is not
 * theirs to recreate through a docker socket anyway).
 */
import type { Container, ContainerInspectInfo } from "dockerode";
import { compareReleaseVersions, isWorkerUpgradeAllowed } from "@metapages/compute-queues-shared";
import humanizeDuration from "humanize-duration";

import { config } from "/@/config.ts";
import { docker } from "/@/queue/dockerClient.ts";
import type { DockerJobQueue } from "/@/queue/DockerJobQueue.ts";

import mod from "../../mod.json" with { type: "json" };
import { binaryDownloadUrl, getLatestReleaseVersion, getQueueLocalWorkers, getWorkerVersionGate } from "./releases.ts";
import { execInPlace, inspectOwnContainer, isCompiledBinary, isInContainer } from "./self.ts";
import { HANDOVER_FROM_ENV, HANDOVER_ID_ENV, successorImage, successorSpec } from "./successor.ts";

const VERSION: string = mod.version;

const POLL_INTERVAL = 3_000;
/** How long a successor gets to start, connect and register. */
const SUCCESSOR_REGISTER_DEADLINE = 3 * 60_000;
/** How long a retiring worker waits for its successor to stop it. */
const SUCCESSOR_TAKEOVER_DEADLINE = 3 * 60_000;
/** Backoff ceiling, as a multiple of the check interval. */
const MAX_BACKOFF_FACTOR = 24;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

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
      next = args.intervalMs * Math.min(2 ** failures, MAX_BACKOFF_FACTOR);
      log(`💥 upgrade failed, still running ${VERSION}, next try in ${humanizeDuration(next)}: ${err}`);
    }
    setTimeout(check, next);
  };
  // Soon after start, not immediately: let the worker connect first.
  setTimeout(check, Math.min(args.intervalMs, 60_000));
};

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

/** Wait until the successor is on the queue: same worker id, the new version, in standby. */
const verifySuccessor = async (args: AutoUpdateArgs, successor: Container, version: string) => {
  const deadline = Date.now() + SUCCESSOR_REGISTER_DEADLINE;
  while (Date.now() < deadline) {
    await assertRunning(successor, "before registering");
    try {
      const workers = await getQueueLocalWorkers(args.server, args.queueId);
      if (workers.some((w) => w.id === args.workerId && w.version === version && w.handover === "standby")) {
        log(`successor ${version} is registered on queue ${args.queueId} ✅`);
        return;
      }
    } catch (err) {
      log(`cannot read the queue status yet: ${err}`);
    }
    await sleep(POLL_INTERVAL);
  }
  throw new Error(
    `the successor did not register on queue ${args.queueId} within ${
      humanizeDuration(SUCCESSOR_REGISTER_DEADLINE)
    }:\n${await containerLogsTail(
      successor,
    )}`,
  );
};

/**
 * Stop claiming jobs, then wait until the running ones can be left behind:
 * - adopt (job containers on the host's docker): every job's container is
 *   running, so the successor adopts it. Jobs still copying inputs, building
 *   or uploading outputs are let through that phase first.
 * - drain (standalone: job containers die with this container's dockerd):
 *   every job has finished.
 * Bounded by --max-job-duration: no job outlives that anyway.
 */
const drain = async (args: AutoUpdateArgs, mode: "adopt" | "drain", successor?: Container) => {
  args.queue.enterHandover("draining");
  const deadline = Date.now() + args.maxJobDurationMs + 60_000;
  let lastLogged = 0;
  while (Date.now() < deadline) {
    const done = mode === "adopt" ? args.queue.isSafeToHandOverRunningJobs() : args.queue.runningJobCount() === 0;
    if (done) {
      return;
    }
    if (Date.now() - lastLogged > 60_000) {
      lastLogged = Date.now();
      log(
        mode === "adopt"
          ? `waiting for jobs to reach a running container before handing them over`
          : `waiting for ${args.queue.runningJobCount()} running job(s) to finish (standalone: they cannot be handed over)`,
      );
    }
    if (successor) {
      await assertRunning(successor, "while this worker was draining");
    }
    await sleep(POLL_INTERVAL);
  }
  log(`draining took longer than --max-job-duration, handing over anyway`);
};

const upgradeContainer = async (args: AutoUpdateArgs, version: string) => {
  const self = await inspectOwnContainer();
  if (!self) {
    throw new Error("cannot inspect this worker's own container");
  }
  const standalone = isStandalone();
  const image = successorImage(self.Config.Image, version, standalone);
  log(`pulling ${image}`);
  await pullImage(image);

  const currentImage = await docker.getImage(self.Image).inspect().catch(() => undefined);
  const baseName = self.Name.replace(/^\//, "");
  const name = `${baseName}-${version}`.replace(/[^a-zA-Z0-9_.-]/g, "-");
  // a leftover from an earlier attempt that did not clean up
  await docker.getContainer(name).remove({ force: true }).catch(() => {});

  const spec = successorSpec({
    current: self,
    currentImage,
    image,
    name,
    env: { [HANDOVER_FROM_ENV]: self.Id, [HANDOVER_ID_ENV]: args.workerId },
  });
  const successor = await docker.createContainer(spec.options);
  log(`created successor ${name} (${successor.id.slice(0, 12)})`);
  try {
    for (const network of spec.extraNetworks) {
      await docker.getNetwork(network.name).connect({ Container: successor.id, EndpointConfig: network.config });
    }
    await successor.start();
    await verifySuccessor(args, successor, version);
    await drain(args, standalone ? "drain" : "adopt", successor);
    args.queue.enterHandover("retiring");
    log(`ready to be replaced, waiting for ${name} to stop this container`);
    const deadline = Date.now() + SUCCESSOR_TAKEOVER_DEADLINE;
    while (Date.now() < deadline) {
      await assertRunning(successor, "before taking over");
      await sleep(POLL_INTERVAL);
    }
    throw new Error(`the successor did not take over within ${humanizeDuration(SUCCESSOR_TAKEOVER_DEADLINE)}`);
  } catch (err) {
    log(`abandoning the upgrade, removing ${name}`);
    await successor.remove({ force: true }).catch((removeErr) => log(`could not remove ${name}: ${removeErr}`));
    await args.queue.exitHandover();
    throw err;
  }
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

  // Job containers run on the host's docker and outlive this process; the
  // re-exec'd worker has the same id and adopts them.
  await drain(args, "adopt");
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
    const info = await predecessor.inspect().catch(() => null);
    let ready = !info?.State?.Running;
    if (!ready) {
      try {
        const workers = await getQueueLocalWorkers(args.server, args.queueId);
        ready = workers.some((w) => w.id === args.workerId && w.handover === "retiring");
      } catch (err) {
        if (config.debug) {
          log(`cannot read the queue status: ${err}`);
        }
      }
    }
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
