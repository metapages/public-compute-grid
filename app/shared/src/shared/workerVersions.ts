import { getEnv } from "./env.ts";

/**
 * Which worker versions a server will let workers upgrade to. Served at
 * WORKER_VERSIONS_PATH and read by a worker running with --auto-update before
 * it starts a successor: the server goes first, so a worker never moves to a
 * release the server it talks to does not know about yet.
 */
export interface WorkerVersionGate {
  /** The worker version this server was built alongside. */
  server: string;
  /** Workers older than this are refused an upgrade target below it. */
  minimum?: string;
  /** The newest worker version the server accepts. Defaults to `server`. */
  maximum?: string;
}

export const WORKER_VERSIONS_PATH = "/api/v1/worker/versions";

/**
 * Parse a release version: `1.2.3` or `v1.2.3`. Pre-releases (`1.2.3-rc.1`)
 * and anything else return null, so callers never move onto them.
 */
export const parseReleaseVersion = (version: string): [number, number, number] | null => {
  const match = /^v?(\d+)\.(\d+)\.(\d+)$/.exec(version?.trim() ?? "");
  if (!match) {
    return null;
  }
  return [parseInt(match[1], 10), parseInt(match[2], 10), parseInt(match[3], 10)];
};

/** Semver order of two release versions; unparseable versions sort first. */
export const compareReleaseVersions = (a: string, b: string): number => {
  const pa = parseReleaseVersion(a);
  const pb = parseReleaseVersion(b);
  if (!pa || !pb) {
    return pa ? 1 : pb ? -1 : 0;
  }
  for (let i = 0; i < 3; i++) {
    if (pa[i] !== pb[i]) {
      return pa[i] - pb[i];
    }
  }
  return 0;
};

/** The gate this server serves: env overrides, otherwise its own version as the ceiling. */
export const getWorkerVersionGate = (serverVersion: string): WorkerVersionGate => {
  const minimum = getEnv("WORKER_VERSION_MINIMUM") || undefined;
  const maximum = getEnv("WORKER_VERSION_MAXIMUM") || serverVersion;
  return { server: serverVersion, minimum, maximum };
};

/**
 * May a worker on `from` move to `to` under this gate? Only forward, only onto
 * a release version, and only inside the server's [minimum, maximum].
 */
export const isWorkerUpgradeAllowed = (
  gate: WorkerVersionGate | null | undefined,
  from: string,
  to: string,
): { allowed: boolean; reason: string } => {
  if (!parseReleaseVersion(to)) {
    return { allowed: false, reason: `${to} is not a release version` };
  }
  if (parseReleaseVersion(from) && compareReleaseVersions(to, from) <= 0) {
    return { allowed: false, reason: `${to} is not newer than ${from}` };
  }
  if (!gate) {
    return { allowed: false, reason: "the server does not publish a worker version gate" };
  }
  const maximum = gate.maximum || gate.server;
  if (maximum && parseReleaseVersion(maximum) && compareReleaseVersions(to, maximum) > 0) {
    return { allowed: false, reason: `the server accepts workers up to ${maximum}, not ${to} yet` };
  }
  if (gate.minimum && parseReleaseVersion(gate.minimum) && compareReleaseVersions(to, gate.minimum) < 0) {
    return { allowed: false, reason: `the server requires at least ${gate.minimum}` };
  }
  return { allowed: true, reason: `the server accepts ${to}` };
};
