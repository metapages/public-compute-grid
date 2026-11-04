import { ContainerLabel, ContainerLabelId, ContainerLabelQueue, ContainerLabelWorker } from "/@/queue/constants.ts";
import { docker } from "/@/queue/dockerClient.ts";
import type Docker from "dockerode";

import { getJobColorizedString, getQueueColorizedString } from "@metapages/compute-queues-shared";

import { config } from "../config.ts";
import { DockerRunPhase, type WorkerJobQueueItem } from "./types.ts";
import { getDockerFiltersForJob } from "./utils.ts";

export const removeAllDeadContainersFromQueue = async (args: { queue: string; workerId: string }) => {
  const { queue, workerId } = args;
  if (config.DebugDisableContainerDeletion) {
    // console.log(
    //   `${getQueueColorizedString(queue)}
    //   } ❗ DebugDisableContainerDeletion is true, skipping removeAllDeadContainersFromQueue`,
    // );
    return;
  }

  // Check for existing job container
  const runningContainers = await docker.listContainers({
    all: true,
    filters:
      `{"label": ["${ContainerLabel}=true", "${ContainerLabelQueue}=${queue}", "${ContainerLabelWorker}=${workerId}"], "status": ["exited"]}`,
  });
  for (const containerData of runningContainers) {
    const containerId = containerData.Id;
    const containerLabels = containerData.Labels;
    const jobId = containerLabels[ContainerLabelId];
    // jobs created during functional tests are ignored
    // const containerTestMode = containerLabels[ContainerLabelTestMode];
    // if (containerTestMode !== "true") {
    //   continue;
    // }
    try {
      const container = docker.getContainer(containerId);

      await container.remove({ v: true });
      console.log(
        `${getQueueColorizedString(queue)} ${getJobColorizedString(jobId)} :: Removed dead queue container ${
          containerId.substring(
            0,
            8,
          )
        } from queue ${queue}`,
      );
    } catch (_err) {
      /* do nothing */
      console.log(
        `Failed to kill container not in queue but ignoring error: ${_err}`,
      );
    }
  }
};

export const removeAllJobsFromQueueNotInSet = async (args: { queue: string; workerId: string; set: Set<string> }) => {
  const { queue, workerId, set } = args;
  // Check for existing job container
  const runningContainers = await docker.listContainers({
    filters:
      `{"label": ["${ContainerLabel}=true", "${ContainerLabelQueue}=${queue}", "${ContainerLabelWorker}=${workerId}"]}`,
  });
  for (const containerData of runningContainers) {
    const containerId = containerData.Id;
    const containerLabels = containerData.Labels;
    const jobId = containerLabels[ContainerLabelId];
    const containerQueue = containerLabels[ContainerLabelQueue];
    if (jobId && !set.has(jobId)) {
      try {
        const container = docker.getContainer(containerId);
        console.log(
          `${getQueueColorizedString(queue)} ${getJobColorizedString(jobId)} 💀💀💀 Killing container ${containerId}`,
        );
        await container.kill();
        if (config.DebugDisableContainerDeletion) {
          console.log(
            `${getQueueColorizedString(queue)} ${
              getJobColorizedString(jobId)
            } ❗ DebugDisableContainerDeletion is true, skipping container removal`,
          );
          continue;
        }
        await container.remove({ v: true });
      } catch (_err) {
        /* do nothing */
        console.log(
          `Failed to kill dangling job container but ignoring error: ${_err}`,
        );
      }
      console.log(
        `Removed dangling job ${
          jobId.substring(
            0,
            8,
          )
        } from queue ${containerQueue}`,
      );
    }
  }
};

export const killAndRemove = async (
  workItem: WorkerJobQueueItem,
  jobId: string,
  container: Docker.Container,
  reason: string,
): Promise<unknown> => {
  if (workItem) {
    workItem.phase = DockerRunPhase.Ended;
  }
  if (!container) {
    console.log(`${getJobColorizedString(jobId)} 🗑️ killAndRemove(${reason}) no container!`);
    return;
  }
  console.log(`${getJobColorizedString(jobId)} 🗑️ killAndRemove(${reason}) Cleaning up container ${container?.id}`);
  if (container) {
    let containerInfo: Docker.ContainerInspectInfo | undefined;
    try {
      containerInfo = await container.inspect();
    } catch (error) {
      // not found
      if (error instanceof Error && error.message.includes("HTTP code 404")) {
        return;
      }
      console.log(
        `${getJobColorizedString(jobId)} killAndRemove Failed to inspect container ${container.id} but ignoring error:`,
        error,
      );
    }

    const status = containerInfo?.State?.Status;
    if (status === "removing") {
      console.log(
        `${getJobColorizedString(jobId)} Container ${container.id} is already being removed, skipping cleanup`,
      );
      return;
    }
    const okToKill = status === "running" || status === "restarting" || status === "paused";
    if (okToKill) {
      console.log(`${getJobColorizedString(jobId)} container ${container.id} is running, killing before removing`);
      try {
        console.log(`${getJobColorizedString(jobId)} 💀💀💀 Killing container because ${reason} ${container.id}`);
        await container.kill();
      } catch (_err) {
        /* do nothing */
      }
    }

    // status = containerInfo?.State?.Status;
    // if (status === "removing") {
    //   return;
    // }
    try {
      if (config.DebugDisableContainerDeletion) {
        console.log(
          `${getJobColorizedString(jobId)} ❗ DebugDisableContainerDeletion is true, skipping container removal`,
        );
        return;
      }
      await container.remove({ v: true });
      console.log(`${getJobColorizedString(jobId)} ✅ Successfully removed container ${container.id}`);
    } catch (err) {
      if (!(err as Error).message.includes("already in progress")) {
        console.log(
          `${getJobColorizedString(jobId)} Failed to remove but ignoring error: ${err}`,
        );
      }
    }
  }
};

export const killAndRemoveContainerForJob = async (
  args: { queue: string; workerId: string; jobId: string },
): Promise<void> => {
  const { queue, jobId } = args;
  const runningContainers = await docker.listContainers({
    all: true,
    filters: getDockerFiltersForJob(args),
    // `{"label": ["${ContainerLabel}=true", "${ContainerLabelId}=${jobId}", "${ContainerLabelQueue}=${queue}", "${ContainerLabelWorker}=${workerId}"]}`,
  });
  for (const containerData of runningContainers) {
    const containerId = containerData.Id;
    try {
      const container = docker.getContainer(containerId);
      if (!container) {
        continue;
      }

      const containerInfo = await container.inspect();
      if (containerInfo.State?.Status === "removing") {
        console.log(
          `${getJobColorizedString(jobId)} container ${container.id} is already being removed, skipping cleanup`,
        );
        continue;
      }
      if (
        containerInfo.State?.Status === "restarting" || containerInfo.State?.Status === "running" ||
        containerInfo.State?.Status === "paused"
      ) {
        try {
          console.log(
            `${getQueueColorizedString(queue)} ${
              getJobColorizedString(jobId)
            } ☠️💀☠️ Killing container ${container.id}`,
          );
          await container.kill();
        } catch (_) {
          // ignore
        }
      }
      if (config.DebugDisableContainerDeletion) {
        console.log(
          `${getQueueColorizedString(queue)} ${
            getJobColorizedString(jobId)
          } ❗ DebugDisableContainerDeletion is true, skipping container removal`,
        );
        continue;
      }

      await container.remove({ v: true });
      console.log(
        `${getJobColorizedString(jobId)} :: Removed dead queue container ${containerId} `,
      );
    } catch (err) {
      /* do nothing */
      if (!(err as Error).message.includes("already in progress")) {
        console.log(
          `${getJobColorizedString(jobId)} Failed to remove but ignoring error: ${err}`,
        );
      }
    }
  }
};

export const killAnyStoppedContainersForJob = async (
  args: { workerId: string; jobId: string },
): Promise<void> => {
  const { jobId } = args;
  if (config.DebugDisableContainerDeletion) {
    console.log(
      `${
        getJobColorizedString(jobId)
      } ❗ DebugDisableContainerDeletion is true, skipping killAnyStoppedContainersForJob`,
    );
    return;
  }

  console.log(`${getJobColorizedString(jobId)} 🗑️ cleaning up stopped containers`);
  const runningContainers = await docker.listContainers({
    all: true,
    filters: getDockerFiltersForJob({ ...args, status: "exited" }),
    // filters:
    //   `{"label": ["${ContainerLabel}=true", "${ContainerLabelId}=${jobId}", "${ContainerLabelWorker}=${workerId}"], "status": ["exited"]}`,
  });
  for (const containerData of runningContainers) {
    const containerId = containerData.Id;
    try {
      const container = docker.getContainer(containerId);
      if (!container) {
        continue;
      }

      const containerInfo = await container.inspect();
      if (containerInfo.State?.Status === "removing") {
        console.log(
          `${getJobColorizedString(jobId)} container ${container.id} is already being removed, skipping cleanup`,
        );
        continue;
      }
      if (
        containerInfo.State?.Status === "restarting" || containerInfo.State?.Status === "running" ||
        containerInfo.State?.Status === "paused"
      ) {
        continue;
      }

      await container.remove({ v: true });
      console.log(
        `${getJobColorizedString(jobId)} :: Removed dead queue container ${containerId} `,
      );
    } catch (err) {
      /* do nothing */
      if (!(err as Error).message.includes("already in progress")) {
        console.log(
          `${getJobColorizedString(jobId)} Failed to remove but ignoring error: ${err}`,
        );
      }
    }
  }
};
