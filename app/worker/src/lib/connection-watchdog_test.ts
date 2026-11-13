import { assert, assertEquals, assertStringIncludes } from "std/assert";

import {
  checkConnection,
  type ClockReading,
  type ConnectionTimes,
  PingStats,
  suspendedFor,
  type WatchdogInput,
} from "/@/lib/connection-watchdog.ts";

const SILENCE = 90_000;
const NOT_OPEN = 90_000;

/** A check 3s after the previous one, with no suspend in between. */
const input = (
  overrides: Partial<Omit<WatchdogInput, "times">> & { at?: number; times?: Partial<ConnectionTimes> },
): WatchdogInput => {
  const at = overrides.at ?? 1_000_000;
  const now: ClockReading = overrides.now ?? { monotonic: at, wall: 5_000_000 + at };
  const previous: ClockReading = overrides.previous ??
    { monotonic: now.monotonic - 3_000, wall: now.wall - 3_000 };
  return {
    now,
    previous,
    isOpen: overrides.isOpen ?? true,
    times: { lastPong: at, lastMessage: at, lastOpen: at, ...overrides.times },
    silenceTolerance: overrides.silenceTolerance ?? SILENCE,
    notOpenTolerance: overrides.notOpenTolerance ?? NOT_OPEN,
  };
};

Deno.test("watchdog: an OPEN socket with a recent PONG is left alone", () => {
  assertEquals(checkConnection(input({ times: { lastPong: 1_000_000 - 5_000 } })), { reconnect: false });
});

Deno.test("watchdog: an OPEN socket with no PONG for the tolerance reconnects", () => {
  const decision = checkConnection(input({ times: { lastPong: 1_000_000 - SILENCE } }));
  assert(decision.reconnect);
  assertStringIncludes(decision.reason, "no PONG");
});

Deno.test("watchdog: a socket stuck CONNECTING/CLOSING past the tolerance reconnects", () => {
  // Before #315 both branches required OPEN, so this spun forever.
  const decision = checkConnection(
    input({ isOpen: false, times: { lastOpen: 1_000_000 - NOT_OPEN, lastPong: 0, lastMessage: 0 } }),
  );
  assert(decision.reconnect);
  assertStringIncludes(decision.reason, "not OPEN");
});

Deno.test("watchdog: a socket briefly not OPEN (normal reconnect backoff) is left alone", () => {
  assertEquals(
    checkConnection(input({ isOpen: false, times: { lastOpen: 1_000_000 - 12_000, lastPong: 0 } })),
    { reconnect: false },
  );
});

Deno.test("watchdog: a host suspend is reported as a suspend, not as silence", () => {
  // The #315 log: 15m33s of wall clock passed in a single 3s check.
  const suspended = 15 * 60_000 + 33_000;
  const now = { monotonic: 1_000_000, wall: 50_000_000 };
  const previous = { monotonic: now.monotonic - 3_000, wall: now.wall - 3_000 - suspended };
  assertEquals(suspendedFor(previous, now), suspended);
  const decision = checkConnection(input({ now, previous }));
  assert(decision.reconnect);
  assertEquals(decision.suspendedMs, suspended);
  assertStringIncludes(decision.reason, "suspended");
});

Deno.test("watchdog: ordinary timer jitter is not a suspend", () => {
  const now = { monotonic: 1_000_000, wall: 50_000_000 };
  const previous = { monotonic: now.monotonic - 3_000, wall: now.wall - 4_500 };
  assertEquals(checkConnection(input({ now, previous })), { reconnect: false });
});

Deno.test("watchdog: OPEN but no messages at all for 3x the tolerance reconnects", () => {
  const decision = checkConnection(
    input({ silenceTolerance: 1_000_000, times: { lastMessage: 1_000_000 - 3_000_000, lastPong: 1_000_000 } }),
  );
  assert(decision.reconnect);
  assertStringIncludes(decision.reason, "no messages");
});

Deno.test("PingStats: answered pings log nothing, unanswered ones log rate-limited", () => {
  const stats = new PingStats(60_000);
  assertEquals(stats.onPing(0), undefined);
  stats.onPong();
  assertEquals(stats.onPing(30_000), undefined);
  // Server goes quiet: the second unanswered ping is worth a line...
  const line = stats.onPing(60_000);
  assert(line);
  assertStringIncludes(line, "PING #3");
  assertStringIncludes(line, "1 earlier ping(s) unanswered");
  // ...but not one every ping.
  assertEquals(stats.onPing(90_000), undefined);
  assert(stats.onPing(120_000));
  assertEquals(stats.summary(), "pings sent=5, pongs=1, unanswered since last PONG=4");
  stats.onPong();
  assertEquals(stats.onPing(130_000), undefined);
});
