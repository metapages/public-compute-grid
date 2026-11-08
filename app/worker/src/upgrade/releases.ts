import {
  compareReleaseVersions,
  parseReleaseVersion,
  WORKER_VERSIONS_PATH,
  type WorkerStatusResponse,
  type WorkerVersionGate,
} from "@metapages/compute-queues-shared";

const GITHUB_REPOSITORY = "metapages/public-compute-grid";
const DOCKER_HUB_REPOSITORY = "metapage/metaframe-docker-worker";
/** The binary asset each release carries (linux x86_64). */
export const BINARY_ASSET_NAME = "metapage-worker";

/**
 * Pins the version --auto-update moves to instead of asking GitHub and Docker
 * Hub. For holding a fleet on a release, and for testing against locally
 * built images.
 */
export const AUTO_UPDATE_VERSION_ENV = "METAPAGE_IO_WORKER_AUTO_UPDATE_VERSION";

const fetchJson = async (url: string): Promise<unknown> => {
  const response = await fetch(url, {
    headers: { "Accept": "application/json", "User-Agent": "metapage-worker" },
    signal: AbortSignal.timeout(20_000),
  });
  if (!response.ok) {
    await response.body?.cancel();
    throw new Error(`${url} answered ${response.status}`);
  }
  return response.json();
};

/**
 * The newest published worker release, or null if it cannot be found out.
 * GitHub's "latest release" already excludes drafts and pre-releases; Docker
 * Hub is the fallback (GitHub rate limits anonymous calls to 60/hour/IP).
 */
export const getLatestReleaseVersion = async (): Promise<string | null> => {
  const pinned = Deno.env.get(AUTO_UPDATE_VERSION_ENV);
  if (pinned) {
    return parseReleaseVersion(pinned) ? pinned.replace(/^v/, "") : null;
  }
  try {
    const release = await fetchJson(
      `https://api.github.com/repos/${GITHUB_REPOSITORY}/releases/latest`,
    ) as { tag_name?: string; prerelease?: boolean; draft?: boolean };
    if (release?.tag_name && !release.prerelease && !release.draft && parseReleaseVersion(release.tag_name)) {
      return release.tag_name.replace(/^v/, "");
    }
  } catch (err) {
    console.log(`🔁 auto-update: GitHub releases unavailable (${err}), trying Docker Hub`);
  }
  try {
    const tags = await fetchJson(
      `https://hub.docker.com/v2/repositories/${DOCKER_HUB_REPOSITORY}/tags?page_size=100&ordering=last_updated`,
    ) as { results?: { name: string }[] };
    const versions = (tags?.results ?? []).map((t) => t.name).filter((name) => parseReleaseVersion(name));
    versions.sort(compareReleaseVersions);
    return versions.at(-1)?.replace(/^v/, "") ?? null;
  } catch (err) {
    console.log(`🔁 auto-update: Docker Hub tags unavailable (${err})`);
  }
  return null;
};

/** The server's worker version gate, or null if it does not publish one (too old to upgrade against). */
export const getWorkerVersionGate = async (server: string): Promise<WorkerVersionGate | null> => {
  try {
    return await fetchJson(`${server}${WORKER_VERSIONS_PATH}`) as WorkerVersionGate;
  } catch (err) {
    console.log(`🔁 auto-update: no worker version gate at ${server}${WORKER_VERSIONS_PATH} (${err})`);
    return null;
  }
};

/** The workers connected to this queue's server, as `/q/<queue>/status` reports them. */
export const getQueueLocalWorkers = async (server: string, queue: string): Promise<WorkerStatusResponse[]> => {
  const status = await fetchJson(`${server}/q/${queue}/status`) as {
    localWorkers?: Record<string, WorkerStatusResponse>;
  };
  return Object.values(status?.localWorkers ?? {});
};

export const binaryDownloadUrl = (version: string) =>
  `https://github.com/${GITHUB_REPOSITORY}/releases/download/${version}/${BINARY_ASSET_NAME}`;
