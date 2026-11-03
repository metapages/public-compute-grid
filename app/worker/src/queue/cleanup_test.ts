import { assertEquals } from "std/assert";
import type Docker from "dockerode";

import {
  killAndRemove,
  killAndRemoveContainerForJob,
  killAnyStoppedContainersForJob,
  removeAllDeadContainersFromQueue,
  removeAllJobsFromQueueNotInSet,
} from "/@/queue/cleanup.ts";
import { ContainerLabelId } from "/@/queue/constants.ts";
import { docker } from "/@/queue/dockerClient.ts";
import type { WorkerJobQueueItem } from "/@/queue/types.ts";

// An image's VOLUME lines give its container anonymous volumes. Nothing can reuse
// them once it is gone, and nothing ties them to a stack, so `v` has to go with
// every removal or they pile up for good.
const removalOptions = async (removeContainer: (container: Docker.Container) => Promise<unknown>) => {
  const removed: unknown[] = [];
  const container = {
    id: "container",
    inspect: () => Promise.resolve({ State: { Status: "exited" } }),
    kill: () => Promise.resolve(),
    remove: (options: unknown) => {
      removed.push(options);
      return Promise.resolve();
    },
  } as unknown as Docker.Container;

  const { listContainers, getContainer } = docker;
  docker.listContainers = (() =>
    Promise.resolve([{
      Id: "container",
      Labels: { [ContainerLabelId]: "job" },
    }])) as unknown as typeof docker.listContainers;
  docker.getContainer = (() => container) as typeof docker.getContainer;
  try {
    await removeContainer(container);
  } finally {
    docker.listContainers = listContainers;
    docker.getContainer = getContainer;
  }
  return removed;
};

const removals: Record<string, (container: Docker.Container) => Promise<unknown>> = {
  killAndRemove: (container) => killAndRemove({} as WorkerJobQueueItem, "job", container, "test"),
  killAndRemoveContainerForJob: () => killAndRemoveContainerForJob({ queue: "q", workerId: "w", jobId: "job" }),
  killAnyStoppedContainersForJob: () => killAnyStoppedContainersForJob({ workerId: "w", jobId: "job" }),
  removeAllDeadContainersFromQueue: () => removeAllDeadContainersFromQueue({ queue: "q", workerId: "w" }),
  removeAllJobsFromQueueNotInSet: () => removeAllJobsFromQueueNotInSet({ queue: "q", workerId: "w", set: new Set() }),
};

for (const [name, removeContainer] of Object.entries(removals)) {
  Deno.test(`${name} removes the container's anonymous volumes with it`, async () => {
    assertEquals(await removalOptions(removeContainer), [{ v: true }]);
  });
}
