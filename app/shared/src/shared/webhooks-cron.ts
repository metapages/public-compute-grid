import { retryUnsuccessfulWebhooks } from "@shared/webhooks.ts";

/**
 * Scheduled retry of webhooks that failed their first attempt.
 *
 * This lives in its own module, imported for its side effect, because of two
 * constraints that pull in opposite directions:
 *
 * 1. Deno.cron must be lexically at the top level of a module. Deno Deploy
 *    discovers cron definitions by evaluating only the top-level scope in an
 *    ephemeral isolate, so a definition nested inside a function errors or,
 *    worse, is silently ignored. It cannot be wrapped in a start() function.
 *
 * 2. It must not register in the test runner. This used to sit at the top of
 *    webhooks.ts, which db.ts imports, so every process touching db.ts got the
 *    cron -- including `deno test`, which runs with --unstable-cron. There it
 *    fired on the minute against a Kv the tests open and close themselves, and
 *    the unawaited rejection surfaced as an uncaught error against whatever
 *    unrelated test module happened to be running.
 *
 * Separating the module satisfies both: the definition stays top-level, and
 * only the long-running servers import it. Import it from a server entrypoint:
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
