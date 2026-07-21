import { removeAllDeadContainersFromQueue } from "/@/queue/cleanup.ts";

import { config } from "../config.ts";
import { ensureIsolateNetwork } from "./network.ts";
import { ensureSharedVolume } from "./volume.ts";

export async function ensureResourcesAndCleanseUnknownJobContainers(
  queue: string,
) {
  await ensureSharedVolume();
  await ensureIsolateNetwork();
  await removeAllDeadContainersFromQueue({ queue, workerId: config.id });
}

export const prepGpus = async (gpus: number | undefined) => {
  // If GPUs are configured, run ldconfig
  if (gpus && gpus > 0) {
    try {
      // https://github.com/NVIDIA/nvidia-docker/issues/1399
      Deno.stdout.writeSync(
        new TextEncoder().encode(
          `[gpus=${gpus}] rebuild ldconfig cache...`,
        ),
      );
      const ldconfig = new Deno.Command("ldconfig");
      await ldconfig.output();
      console.log("✅");
    } catch (err) {
      console.log(`⚠️ ldconfig error ${`${err}`.split(":")[0]}`);
    }
  }
};

export const runChecksOnInterval = async (
  queue: string,
  interval: number = 5000,
) => {
  await ensureResourcesAndCleanseUnknownJobContainers(queue);
  let intervalId: ReturnType<typeof setInterval> | undefined = setInterval(async () => {
    await ensureResourcesAndCleanseUnknownJobContainers(queue);
  }, interval);

  const cleanup = () => {
    if (intervalId) {
      clearInterval(intervalId);
      intervalId = undefined;
    }
  };

  Deno.addSignalListener("SIGINT", cleanup);
  Deno.addSignalListener("SIGTERM", cleanup);
};
