Deno.cron("Check for webhooks to retry", "* * * * *", () => {
  retryUnsuccessfulWebhooks();
});

import { JobDataCacheDurationMilliseconds } from "./constants.ts";
import { getKv } from "@shared/kv.ts";
import type { DockerJobControlConfig } from "@shared/types.ts";
import { getJobColorizedString, getQueueColorizedString } from "@shared/util.ts";

export const callJobWebhook = async (
  queue: string,
  namespace: string,
  jobId: string,
  config: DockerJobControlConfig,
) => {
  // console.log(
  //   `🔥🔥 callJobWebhook ${getJobColorizedString(jobId)} `,
  //   queue,
  //   namespace,
  // );
  const webhookUrl = config.callbacks?.queued?.url;
  if (!webhookUrl) {
    console.log(
      `${getQueueColorizedString(queue)} ${
        getJobColorizedString(jobId)
      } [namespace=${namespace}] callJobWebhook but ‼️ no webhook url, unregistering`,
    );
    await deleteJobProcessSubmissionWebhook(queue, namespace, jobId);
    return;
  }
  const payload = config.callbacks?.queued?.payload || {};

  // console.log(
  //   `🔥🔥 callJobWebhook [${jobId.substring(0, 6)}] webhookUrl=`,
  //   webhookUrl,
  // );
  try {
    const response = await fetch(webhookUrl, {
      redirect: "follow",
      method: "POST",
      body: JSON.stringify({
        jobId,
        queue,
        namespace,
        config: payload,
      }),
      headers: {
        "Content-Type": "application/json",
      },
    });

    if (!response.ok) {
      console.log(
        `${getQueueColorizedString(queue)} ${
          getJobColorizedString(jobId)
        } [namespace=${namespace}] callJobWebhook but ‼️ webhook=[${webhookUrl}] failed with status=[${response.status}]`,
      );
      response?.body?.cancel();
      return;
    }
    await response?.text();
    //record as done
    await deleteJobProcessSubmissionWebhook(queue, namespace, jobId);
    console.log(
      `${getQueueColorizedString(queue)} ${getJobColorizedString(jobId)} [namespace=${namespace}] callJobWebhook ✅`,
    );
  } catch (err) {
    console.error(
      `${getQueueColorizedString(queue)} ${
        getJobColorizedString(jobId)
      } [namespace=${namespace}] callJobWebhook webhook=[$${webhookUrl}}] error, will retry in a minute :`,
      (err?.toString())?.includes("Name or service not known") ? "Name or service not known" : err?.toString(),
    );
    // do not keep test webhooks around
    if (webhookUrl.startsWith("http://test:")) {
      await deleteJobProcessSubmissionWebhook(queue, namespace, jobId);
    }
  }
};

const retryUnsuccessfulWebhooks = async (): Promise<void> => {
  const kv = await getKv();
  const iter = kv.list<DockerJobControlConfig>({
    prefix: ["submission-hook"],
  });
  for await (const res of iter) {
    const { key, value } = res;
    const [_, queue, namespace, jobId] = key as string[];
    const config = value;
    await callJobWebhook(queue, namespace, jobId, config);
  }
};

export const addJobProcessSubmissionWebhook = async (opts: {
  queue: string;
  namespace: string;
  jobId: string;
  control: DockerJobControlConfig;
}): Promise<void> => {
  const { jobId, namespace, queue, control } = opts;

  if (!control?.callbacks?.queued) {
    // console.log(
    //   `👀  addJobProcessSubmissionWebhook ${
    //     getJobColorizedString(jobId)
    //   } no config`,
    // );
    return;
  } else {
    console.log(
      `${getQueueColorizedString(queue)} ${
        getJobColorizedString(jobId)
      } [namespace=${namespace}] 👀 registering onQueue callback`,
    );
  }

  const kv = await getKv();
  await kv.set(["submission-hook", queue, namespace, jobId], control, {
    expireIn: JobDataCacheDurationMilliseconds,
  });
  // awaiting here means the main enqueue function will wait for the webhook to be called
  await callJobWebhook(queue, namespace, jobId, control);
};

const deleteJobProcessSubmissionWebhook = async (
  queue: string,
  namespace: string,
  jobId: string,
): Promise<void> => {
  const kv = await getKv();
  await kv.delete(["submission-hook", queue, namespace, jobId]);
};
