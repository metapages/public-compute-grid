/**
 * The worker's decision of whether its websocket to the server is dead, kept
 * free of timers and sockets so it can be tested (see #315).
 *
 * Silence is measured on a monotonic clock (`performance.now()`), which stops
 * while the host is suspended, while wall-clock time (`Date.now()`) jumps
 * forward on wake. Comparing the two between checks makes a suspend
 * self-evident instead of being reported as minutes of "no PONG".
 */

/** Monotonic milliseconds: does not advance while the host is suspended. */
export const monotonicNow = (): number => performance.now();

/** Wall-clock vs monotonic divergence above this between two checks is a suspend. */
export const SUSPEND_THRESHOLD_MS = 10_000;

export interface ConnectionTimes {
  /** Monotonic time of the last PONG (or of the connection opening). */
  lastPong: number;
  /** Monotonic time of the last message of any kind (or of the connection opening). */
  lastMessage: number;
  /** Monotonic time the socket was last seen OPEN (or was last asked to reconnect). */
  lastOpen: number;
}

export interface ClockReading {
  monotonic: number;
  wall: number;
}

export interface WatchdogInput {
  now: ClockReading;
  /** The reading taken at the previous check, to detect a host suspend. */
  previous: ClockReading;
  isOpen: boolean;
  times: ConnectionTimes;
  /** How long the socket may be OPEN without a PONG. */
  silenceTolerance: number;
  /** How long the socket may be anything other than OPEN (CONNECTING, CLOSING, CLOSED). */
  notOpenTolerance: number;
}

export type WatchdogDecision =
  | { reconnect: false }
  | { reconnect: true; reason: string; suspendedMs?: number };

/**
 * How much longer the wall clock ran than the monotonic clock between two
 * readings: roughly how long the host was suspended in between.
 */
export const suspendedFor = (previous: ClockReading, now: ClockReading): number =>
  (now.wall - previous.wall) - (now.monotonic - previous.monotonic);

export const checkConnection = (input: WatchdogInput): WatchdogDecision => {
  const { now, previous, isOpen, times, silenceTolerance, notOpenTolerance } = input;

  // Timers froze and the server has had all that time to give up on us; the
  // socket may still claim OPEN but should not be trusted.
  const suspendedMs = suspendedFor(previous, now);
  if (suspendedMs >= SUSPEND_THRESHOLD_MS) {
    return {
      reconnect: true,
      reason: `host was suspended for ~${formatMs(suspendedMs)} (wall clock ran ` +
        `${formatMs(now.wall - previous.wall)}, monotonic clock ` +
        `${formatMs(now.monotonic - previous.monotonic)})`,
      suspendedMs,
    };
  }

  if (!isOpen) {
    const notOpenFor = now.monotonic - times.lastOpen;
    if (notOpenFor >= notOpenTolerance) {
      return {
        reconnect: true,
        reason: `socket not OPEN for ${formatMs(notOpenFor)} >= ${formatMs(notOpenTolerance)}`,
      };
    }
    return { reconnect: false };
  }

  const sinceLastPong = now.monotonic - times.lastPong;
  if (sinceLastPong >= silenceTolerance) {
    return {
      reconnect: true,
      reason: `no PONG for ${formatMs(sinceLastPong)} >= ${formatMs(silenceTolerance)}`,
    };
  }

  const sinceLastMessage = now.monotonic - times.lastMessage;
  if (sinceLastMessage >= silenceTolerance * 3) {
    return {
      reconnect: true,
      reason: `no messages for ${formatMs(sinceLastMessage)} >= ${formatMs(silenceTolerance * 3)}`,
    };
  }

  return { reconnect: false };
};

/**
 * Counts pings and pongs so a log can tell "pings went out and were ignored"
 * from "pings never went out", and decides when a ping is worth a log line:
 * always once pings start going unanswered, but at most once per
 * `logEveryMs`, so an idle worker does not fill its log.
 */
export class PingStats {
  sent = 0;
  pongs = 0;
  /** Pings sent since the last PONG. */
  unanswered = 0;
  private lastLogged = -Infinity;

  constructor(private readonly logEveryMs: number) {}

  onPing(nowMonotonic: number): string | undefined {
    this.sent++;
    this.unanswered++;
    // The ping just sent is not overdue yet; the ones before it are.
    if (this.unanswered < 2 || nowMonotonic - this.lastLogged < this.logEveryMs) {
      return undefined;
    }
    this.lastLogged = nowMonotonic;
    return `🌳 ⚠️ PING #${this.sent} sent, ${this.unanswered - 1} earlier ping(s) unanswered ` +
      `(pongs received: ${this.pongs})`;
  }

  /** A new connection starts with nothing outstanding. */
  onOpen(): void {
    this.unanswered = 0;
  }

  onPong(): void {
    this.pongs++;
    this.unanswered = 0;
  }

  summary(): string {
    return `pings sent=${this.sent}, pongs=${this.pongs}, unanswered since last PONG=${this.unanswered}`;
  }
}

const formatMs = (ms: number): string => {
  if (ms < 1000) {
    return `${Math.round(ms)}ms`;
  }
  const seconds = ms / 1000;
  if (seconds < 120) {
    return `${seconds.toFixed(1)}s`;
  }
  return `${Math.floor(seconds / 60)}m${Math.round(seconds % 60)}s`;
};
