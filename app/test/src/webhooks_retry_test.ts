/**
 * A submission webhook that fails is retried by the server's scheduled job
 * (Deno.cron on Deno, a Cron Trigger on Cloudflare), every minute until it gets
 * a 2xx. This exercises that scheduler: the first call fails, a retry must come.
 */
import { assert } from "std/assert";

import { createNewContainerJobMessage, fetchRobust } from "../../shared/src/mod.ts";

import { createWebhookServer } from "./webhooks_test.ts";
import { API_URL, QUEUE_ID } from "./util.ts";

// The scheduler fires once a minute; allow two runs plus slack
const RETRY_TIMEOUT_MS = 150_000;

Deno.test(
  "a failed submission webhook is retried by the scheduler until it succeeds",
  // The local-mode worker serves no submission webhooks
  { ignore: QUEUE_ID === "local" },
  async () => {
    const port = Math.floor(Math.random() * (65535 - 1024) + 1024);
    const namespace = `webhook-retry-${Date.now()}`;
    const calls: number[] = [];
    const { promise: retried, resolve } = Promise.withResolvers<void>();

    const { shutdown } = createWebhookServer({
      path: "/retry",
      port,
      handler: async (req) => {
        await req.json();
        calls.push(Date.now());
        if (calls.length === 1) {
          // First attempt fails: the server must keep the hook and try again
          return new Response("try again later", { status: 503 });
        }
        resolve();
        return new Response("OK");
      },
      timeout: RETRY_TIMEOUT_MS + 10_000,
    });

    try {
      const { queuedJob } = await createNewContainerJobMessage({
        definition: {
          image: "alpine:3.18.5",
          command: `echo webhook-retry-${Math.floor(Math.random() * 1_000_000)}`,
        },
        control: {
          namespace,
          callbacks: { queued: { url: `http://test:${port}/retry`, payload: { retry: true } } },
        },
      });
      const response = await fetchRobust(`${API_URL}/q/${QUEUE_ID}`, {
        method: "POST",
        body: JSON.stringify(queuedJob!.enqueued),
        headers: { "Content-Type": "application/json" },
      });
      await response.text();
      assert(response.ok, `submit failed: ${response.status}`);

      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([
          retried,
          new Promise((_, reject) => {
            timer = setTimeout(
              () => reject(new Error(`no retry within ${RETRY_TIMEOUT_MS}ms (calls: ${calls.length})`)),
              RETRY_TIMEOUT_MS,
            );
          }),
        ]);
      } finally {
        clearTimeout(timer);
      }
      assert(calls.length >= 2, `expected a retry, got ${calls.length} call(s)`);
    } finally {
      await shutdown();
    }
  },
);
