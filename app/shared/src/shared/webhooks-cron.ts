import { retryUnsuccessfulWebhooks } from "@shared/webhooks.ts";

/**
 * Scheduled retry of webhooks that failed their first attempt, for the Deno
 * servers (the docker stacks and the local-mode worker). Cloudflare runs the
 * same retry from its Cron Trigger instead.
 *
 * This lives in its own module, imported for its side effect, so that it does
 * not register in the test runner. It used to sit at the top of webhooks.ts,
 * which db.ts imports, so every process touching db.ts got the cron --
 * including `deno test`, which runs with --unstable-cron. There it fired on the
 * minute against a Kv the tests open and close themselves, and the unawaited
 * rejection surfaced as an uncaught error against whatever unrelated test
 * module happened to be running.
 *
 * Only the long-running servers import it. Import it from a server entrypoint:
 *
 *     import "@shared/webhooks-cron.ts";
 */
Deno.cron("Check for webhooks to retry", "* * * * *", async () => {
  try {
    await retryUnsuccessfulWebhooks();
  } catch (err) {
    // Never let a scheduled retry take down the process it runs in
    console.error("Check for webhooks to retry failed:", err);
  }
});
