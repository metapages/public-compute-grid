import { DockerNetworkForJobs } from "/@/docker/network.ts";
import * as StreamTools from "/@/docker/streamtools.ts";
import { DockerJobSharedVolumeName } from "/@/docker/volume.ts";
import { ContainerLabel, ContainerLabelId, ContainerLabelQueue, ContainerLabelWorker } from "/@/queue/constants.ts";
import { docker } from "/@/queue/dockerClient.ts";
import { DockerBuildError, ensureDockerImage } from "/@/queue/dockerImage.ts";
import bytes from "bytes";
import type { Container, ContainerCreateOptions } from "dockerode";
import type { Buffer } from "std/node/buffer";

import {
  type ConsoleLogLine,
  DockerJobState,
  type DockerRunResult,
  getJobColorizedString,
  getWorkerColorizedString,
  type JobStatusPayload,
  WebsocketMessageTypeWorkerToServer,
} from "@metapages/compute-queues-shared";

import { config } from "../config.ts";
import { killAndRemove } from "./cleanup.ts";
import { type DockerJobArgs, type DockerJobExecution, DockerRunPhase } from "./types.ts";
import { getDockerFiltersForJob } from "./utils.ts";

// Minimal interface for interacting with docker jobs:
//  inputs:
//    - job spec
//    - std streams writables
//  exposed/output:
//      - Promise<result>:
//          - stdout+stderr
//          - exit code
//          - errors
//          - StatusCode
//      - kill switch => Promise<void>
// just that no more.

export const JobCacheDirectory = "/job-cache";

export const dockerJobExecute = (args: DockerJobArgs): DockerJobExecution => {
  const {
    workItem,
    workerId,
    sender,
    id,
    queue,
    image,
    command,
    env,
    workdir,
    shmSize,
    entrypoint,
    volumes,
    deviceRequests,
    maxJobDuration,
    isKilled,
  } = args;

  const result: DockerRunResult = {
    logs: [],
    isTimedOut: false,
  };

  const dockerExecution: DockerJobExecution = {
    kill: (_: string) => {},
    finish: new Promise(() => {}),
    isKilled,
    container: undefined,
  };

  // export interface DockerJobExecution {
  // finish: Promise<DockerRunResult | undefined>;
  // container: Container | undefined;
  // kill: (reason: string) => void | Promise<void>;
  // isKilled: { value: boolean };

  // let container: Container | undefined;

  let durationHandler: number | undefined;

  const kill = async (reason: string) => {
    if (durationHandler) {
      clearInterval(durationHandler);
      durationHandler = undefined;
    }
    if (!finishTime) {
      finishTime = Date.now();
    }
    if (!result.duration && startTime && finishTime) {
      result.duration = finishTime - startTime;
    }

    isKilled.value = true;
    if (dockerExecution.container) {
      console.log(
        `${getWorkerColorizedString(workerId)} ${getJobColorizedString(id)} 🗑️  kill(${reason}) killing container`,
        dockerExecution.container.id,
      );
      await killAndRemove(workItem, id, dockerExecution.container, reason);
    }
  };

  let startTime: number | undefined;
  let finishTime: number | undefined;

  const maxDurationCheck = () => {
    if (startTime && !finishTime && !isKilled.value) {
      const duration = Date.now() - startTime;
      if (duration > maxJobDuration) {
        console.log(
          `${getWorkerColorizedString(workerId)} ${
            getJobColorizedString(id)
          } 💥💥💥 max duration exceeded [${duration} > ${maxJobDuration}]`,
          id,
        );
        result.isTimedOut = true;
        kill("max duration exceeded");
      }
    }
    if (!finishTime && !isKilled.value) {
      setTimeout(maxDurationCheck, 1000);
    }
  };
  maxDurationCheck();

  const createOptions: ContainerCreateOptions = {
    Image: image,
    Cmd: command,
    WorkingDir: workdir,
    Entrypoint: entrypoint,
    HostConfig: {},
    Env: env != null ? Object.keys(env).map((key) => `${key}=${env[key]}`) : [],
    Tty: false, // needed for splitting stdout/err
    AttachStdout: true,
    AttachStderr: true,
    Labels: {
      [ContainerLabelWorker]: workerId,
      [ContainerLabelId]: args.id,
      [ContainerLabelQueue]: queue,
      [ContainerLabel]: "true",
    } as Record<string, string>,
    User: `${Deno.uid()}:${Deno.gid()}`,
  };

  // Connect container to our network
  createOptions.HostConfig!.NetworkMode = DockerNetworkForJobs;

  // this way jobs can notify other resources about how
  // to access this jobs resources e.g. MCP servers.
  createOptions.Env!.push("JOB_ID=" + args.id);
  createOptions.Env!.push("JOB_INPUTS=/inputs");
  createOptions.Env!.push("JOB_OUTPUTS=/outputs");
  createOptions.Env!.push(`JOB_CACHE=${JobCacheDirectory}`);
  createOptions.Env!.push(`JOB_URL_PREFIX=${config?.server}/j/${args.id}`);
  createOptions.Env!.push(`JOB_OUTPUTS_URL_PREFIX=${config?.server}/j/${args.id}/outputs/`);
  createOptions.Env!.push(`JOB_INPUTS_URL_PREFIX=${config?.server}/j/${args.id}/inputs/`);

  // Set CUDA_VISIBLE_DEVICES to 0 because we map assign the GPU index
  // in the docker config, but CUDA will just see the first and only
  // GPU device allocated, so index=0, even though it might not
  // be the first device from the host perspective.
  if (deviceRequests && deviceRequests.length > 0) {
    const gpuDeviceId = deviceRequests[0].DeviceIDs?.[0];
    if (gpuDeviceId !== undefined) {
      // 0 because internally it's the first GPU device allocated
      // even if the host index is different.
      createOptions.Env!.push("CUDA_VISIBLE_DEVICES=0");
    }
  }

  if (deviceRequests) {
    // https://github.com/apocas/dockerode/issues/628
    createOptions.HostConfig!.DeviceRequests = deviceRequests;
  }

  if (volumes != null) {
    createOptions.HostConfig!.Binds = [];
    volumes.forEach((volume) => {
      createOptions.HostConfig!.Binds!.push(
        `${volume.host}:${volume.container}:Z`,
      );
    });
  }

  if (shmSize) {
    createOptions.HostConfig!.ShmSize = bytes(shmSize);
  }

  // Add a volume shared between all job containers
  // For e.g. big downloaded models
  // Security issue? Maybe. Don't store your job
  // data there, store it in /inputs and /outputs.
  createOptions.HostConfig!.Binds!.push(
    `${DockerJobSharedVolumeName}:${JobCacheDirectory}:Z`,
  );

  const grabberOutStream = StreamTools.createTransformStream((s: string) => {
    const log = s.toString();
    result.logs.push([log, Date.now()]);
    sender({
      type: WebsocketMessageTypeWorkerToServer.JobStatusLogs,
      payload: {
        jobId: id,
        step: `${DockerJobState.Running}`,
        logs: [[log, Date.now()]],
      } as JobStatusPayload,
    });
    return s;
  });

  const grabberErrStream = StreamTools.createTransformStream((s: string) => {
    const log = s.toString();
    result.logs.push([log, Date.now(), true]);
    sender({
      type: WebsocketMessageTypeWorkerToServer.JobStatusLogs,
      payload: {
        jobId: id,
        step: `${DockerJobState.Running}`,
        logs: [[log, Date.now(), true]],
      } as JobStatusPayload,
    });
    return s;
  });

  // Nothing reads these transforms' output. Drain it: otherwise each stops
  // accepting input once ~32 KB (its two buffers) backs up, and every log line
  // after a job's first ~32 KB of output is silently lost.
  grabberOutStream.resume();
  grabberErrStream.resume();

  const finish = async () => {
    if (isKilled.value) {
      return;
    }
    if (args.exitedContainer) {
      return await collectExitedContainer(args.exitedContainer);
    }
    if (workItem) {
      workItem.phase = DockerRunPhase.Building;
    }
    try {
      createOptions.Image = await ensureDockerImage({
        jobId: id,
        image,
        build: args.build,
        sender,
      });
    } catch (err: unknown) {
      console.error(
        `${getWorkerColorizedString(workerId)} ${getJobColorizedString(id)} 💥 ensureDockerImage error`,
        (err as Error)?.stack,
      );
      const message = err && typeof err === "object" && "message" in err
        ? err.message
        : `Unknown error: ${String(err)}`;
      result.logs = err &&
          typeof err === "object" &&
          "logs" in err &&
          Array.isArray(err.logs)
        ? err.logs
        : [];
      if (err instanceof DockerBuildError) {
        result.error = "Error building image";
        result.logs.push([`${err.message}`, Date.now(), true]);
        return result;
      } else {
        result.logs.push([
          `Failure to pull or build the docker image:  ${message}`,
          Date.now(),
          true,
        ]);
        result.error = "Error";
        return result;
      }
    }
    if (isKilled.value) {
      return;
    }

    // Check for existing job container
    const existingRunningContainer = await getRunningContainerForJob({ jobId: id, workerId });

    if (isKilled.value) {
      return;
    }

    if (existingRunningContainer) {
      const containerInfo = await existingRunningContainer.inspect();
      if (containerInfo.State?.Status === "running") {
        dockerExecution.container = existingRunningContainer;
      }
    }

    if (isKilled.value) {
      return;
    }

    if (!dockerExecution.container) {
      dockerExecution.container = await docker.createContainer(createOptions);

      if (config.debug) {
        console.log(
          `${getWorkerColorizedString(workerId)} ${getJobColorizedString(id)} 👉 created new container`,
          dockerExecution.container.id,
        );
      }
    }

    // attach to container
    const stream = await dockerExecution.container!.attach({
      stream: true,
      stdout: true,
      stderr: true,
      logs: existingRunningContainer ? true : false,
    });
    // console.log(`🤡 after attach`, containerInfo);
    dockerExecution.container!.modem.demuxStream(stream, grabberOutStream, grabberErrStream);

    if (isKilled.value) {
      return;
    }

    workItem.phase = DockerRunPhase.Running;

    if (!existingRunningContainer) {
      // is buffer
      const _: Buffer = await dockerExecution.container!.start();
      console.log(
        `${getWorkerColorizedString(workerId)} ${getJobColorizedString(id)} 🏎️ container started`,
      );
    }

    startTime = Date.now();

    // Wait for container to finish with enhanced logging
    console.log(
      `${getWorkerColorizedString(workerId)} ${getJobColorizedString(id)} ⏳ Waiting for container to finish...`,
    );
    const dataWait = await dockerExecution.container!.wait();
    result.StatusCode = dataWait != null ? dataWait.StatusCode : null;

    // Enhanced status code analysis
    if (dataWait?.StatusCode === 137) {
      // console.log(
      //   `${getWorkerColorizedString(workerId)} ${
      //     getJobColorizedString(id)
      //   } 💥 CONTAINER KILLED (137) - Likely out of memory!`,
      // );
      // console.log(`${getWorkerColorizedString(workerId)} ${getJobColorizedString(id)} 🔍 Debug info:`, {
      //   containerId: container!.id,
      //   jobId: id,
      //   workerId: workerId,
      //   queue: queue,
      //   duration: startTime ? Date.now() - startTime : "unknown",
      //   maxJobDuration: maxJobDuration,
      //   isKilled: isKilled.value,
      // });

      // Add detailed error information to logs
      result.logs.push([
        `💥 Container killed with status code 137 (out of memory or killed by system)`,
        Date.now(),
        true,
      ]);

      result.logs.push([
        `🔍 Container ID: ${dockerExecution.container!.id}`,
        Date.now(),
        true,
      ]);
      result.logs.push([
        `⏱️ Job duration: ${startTime ? Math.round((Date.now() - startTime) / 1000) + "s" : "unknown"}`,
        Date.now(),
        true,
      ]);
    } else if (dataWait?.StatusCode !== 0) {
      console.log(
        `${getWorkerColorizedString(workerId)} ${
          getJobColorizedString(id)
        } ⚠️ Container exited with non-zero status: ${dataWait?.StatusCode}`,
      );
      result.logs.push([
        `⚠️ Container exited with status code: ${dataWait?.StatusCode}`,
        Date.now(),
        true,
      ]);
    }

    return finished(dataWait?.StatusCode);
  };

  // The container is left in place: DockerJobQueue removes it once the result
  // has been reported. Until then it is the result, and a worker restarted in
  // between collects it again (see collectExitedContainer).
  const finished = (statusCode: number | undefined) => {
    finishTime = finishTime || Date.now();
    if (!result.duration && finishTime && startTime) {
      result.duration = finishTime - startTime;
    }

    if (workItem) {
      workItem.phase = DockerRunPhase.UploadOutputs;
    }

    console.log(`${getWorkerColorizedString(workerId)} ${getJobColorizedString(id)} ✅ container finished`, {
      StatusCode: statusCode,
    });
    return result;
  };

  /**
   * The container ran to the end while no worker was watching it (the worker
   * was down). Its exit code, logs and outputs (on the host, in the job's
   * outputs dir) are the job's result, so read them instead of running it again.
   */
  const collectExitedContainer = async (container: Container) => {
    dockerExecution.container = container;
    workItem.phase = DockerRunPhase.Running;
    const info = await container.inspect();
    // set finishTime first: maxDurationCheck must not judge a finished run
    finishTime = Date.parse(info.State?.FinishedAt) || Date.now();
    startTime = Date.parse(info.State?.StartedAt) || finishTime;
    result.StatusCode = info.State?.ExitCode;
    console.log(
      `${getWorkerColorizedString(workerId)} ${
        getJobColorizedString(id)
      } ♻️ collecting the result of container ${container.id} (exited with ${result.StatusCode} while the worker was down)`,
    );
    const logs = await container.logs({ stdout: true, stderr: true, timestamps: true, follow: false });
    result.logs.push(...parseDockerLogs(logs as unknown as Uint8Array));
    if (isKilled.value) {
      return;
    }
    return finished(result.StatusCode);
  };

  dockerExecution.kill = kill;
  dockerExecution.finish = finish();
  dockerExecution.isKilled = isKilled;

  return dockerExecution;
};

export const getRunningContainerForJob = async (args: {
  jobId: string;
  workerId?: string;
}): Promise<Container | undefined> => {
  const runningContainers = await docker.listContainers({
    filters: getDockerFiltersForJob({ ...args, status: "running" }),
  });
  for (const containerData of runningContainers) {
    try {
      const container = docker.getContainer(containerData.Id);
      if (container) {
        return container;
      }
    } catch (_err) {
      /* do nothing */
      console.log(
        `${getJobColorizedString(args.jobId)} :: Failed to remove stopped container but ignoring error: ${_err}`,
      );
    }
  }
};

/**
 * Killed from outside (SIGKILL/SIGTERM) and not by the OOM killer: a docker or
 * host restart, another worker's orphan check, this worker's own cleanup. The
 * job never finished on its own, so this is no result: run it again. An OOM kill
 * is the job's own doing, and a result.
 */
export const isKilledFromOutside = (state?: { ExitCode?: number; OOMKilled?: boolean }): boolean =>
  (state?.ExitCode === 137 || state?.ExitCode === 143) && !state?.OOMKilled;

/**
 * This worker's newest exited container for the job, if it belongs to the job's
 * current run: created at or after `queuedTime`. A requeue (the server lost the
 * worker) keeps the job's queuedTime, a new submission of the same id gets a new
 * one, so an older container is a result the submitter has already moved past.
 * A container killed from outside is skipped (isKilledFromOutside).
 */
export const getExitedContainerForJob = async (args: {
  jobId: string;
  workerId: string;
  queuedTime: number;
}): Promise<Container | undefined> => {
  const { jobId, workerId, queuedTime } = args;
  const exited = await docker.listContainers({
    all: true,
    filters: getDockerFiltersForJob({ jobId, workerId, status: "exited" }),
  });
  let newest: { container: Container; created: number } | undefined;
  for (const containerData of exited) {
    try {
      const container = docker.getContainer(containerData.Id);
      const info = await container.inspect();
      if (isKilledFromOutside(info.State)) {
        continue;
      }
      // listContainers' Created is in whole seconds, too coarse against queuedTime
      const created = Date.parse(info.Created);
      if (created >= queuedTime && (!newest || created > newest.created)) {
        newest = { container, created };
      }
    } catch (err) {
      console.log(`${getJobColorizedString(jobId)} :: could not inspect exited container ${containerData.Id}: ${err}`);
    }
  }
  return newest?.container;
};

/**
 * Docker's multiplexed log stream (non-TTY containers) with timestamps: frames
 * of an 8 byte header (stream type, 3 bytes padding, uint32 BE length), then
 * "<RFC3339Nano timestamp> <line>".
 */
export const parseDockerLogs = (buffer: Uint8Array): ConsoleLogLine[] => {
  const decoder = new TextDecoder();
  const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
  const lines: ConsoleLogLine[] = [];
  let offset = 0;
  while (offset + 8 <= buffer.length) {
    const isStdErr = buffer[offset] === 2;
    const length = view.getUint32(offset + 4);
    const frame = decoder.decode(buffer.subarray(offset + 8, offset + 8 + length));
    offset += 8 + length;
    const space = frame.indexOf(" ");
    const time = space > 0 ? Date.parse(frame.substring(0, space)) : NaN;
    const text = Number.isNaN(time) ? frame : frame.substring(space + 1);
    const logTime = Number.isNaN(time) ? Date.now() : time;
    lines.push(isStdErr ? [text, logTime, true] : [text, logTime]);
  }
  return lines;
};
