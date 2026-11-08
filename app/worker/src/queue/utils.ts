import { parse, type ParseEntry } from "shell-quote";

import { ContainerLabel, ContainerLabelId, ContainerLabelQueue, ContainerLabelWorker } from "./constants.ts";

export const getDockerFiltersForJob = (
  args: { jobId?: string; workerId?: string; queue?: string; status?: string },
): string => {
  const { jobId, workerId, status, queue } = args;
  const statusFilter = status ? `, "status": ["${status}"]` : "";
  const queueFilter = queue ? `, "${ContainerLabelQueue}=${queue}"` : "";
  const workerFilter = workerId ? `, "${ContainerLabelWorker}=${workerId}"` : "";
  const jobFilter = jobId ? `, "${ContainerLabelId}=${jobId}"` : "";
  return `{"label": ["${ContainerLabel}=true"${jobFilter}${workerFilter}${queueFilter}]${statusFilter}}`;
};

const sanitizeForDockerTag = (input: string): string => {
  return input.replace(/[^a-zA-Z0-9_.-]/g, "-").toLowerCase();
};

const extractOwnerAndRepoName = (
  url: string,
): { owner: string; repo: string } => {
  // Remove the fragment part if it exists
  const urlWithoutFragment = url.split("#")[0];

  // Extract the owner and repository name from the URL
  const urlParts = urlWithoutFragment.replace(/\.git$/, "").split("/");
  const repo = urlParts.pop()!;
  const owner = urlParts.pop()!;
  return { owner, repo };
};

const extractFragment = (url: string): string | null => {
  const fragmentIndex = url.indexOf("#");
  return fragmentIndex !== -1 ? url.substring(fragmentIndex + 1) : null;
};

export function generateDockerImageTag(url: string): string {
  const { owner, repo } = extractOwnerAndRepoName(url);
  const sanitizedOwner = sanitizeForDockerTag(owner);
  const sanitizedRepo = sanitizeForDockerTag(repo);
  const fragment = extractFragment(url);

  if (fragment) {
    const sanitizedFragment = sanitizeForDockerTag(fragment);
    return `${sanitizedOwner}/${sanitizedRepo}:${sanitizedFragment}`;
  } else {
    return `${sanitizedOwner}/${sanitizedRepo}:latest`;
  }
}

export const convertStringToDockerCommand = (
  command: string,
  env?: Record<string, string>,
): string[] | undefined => {
  if (!command) {
    return;
  }
  if (typeof command !== "string") {
    return command;
  }
  const parsed = parse(command, env);
  const containsOperations = parsed.some((item: ParseEntry) => typeof item === "object");
  if (containsOperations) {
    return [command];
  }
  return parsed as string[];
};

// function sanitizeForDockerTag(input: string): string {
//   return input.replace(/[^a-zA-Z0-9_.-]/g, "").toLowerCase();
// }

// function extractRepoName(url: string): string {
//   // Extract the repository name from the URL
//   const urlParts = url.split("/");
//   const repoWithExtension = urlParts[urlParts.length - 1];
//   const repoName = repoWithExtension.replace(/\.git$/, "");
//   return repoName;
// }

// export function generateDockerImageTag(url: string): string {
//   const sanitizedUrl = sanitizeForDockerTag(url);
//   const repoName = extractRepoName(url);
//   const sanitizedRepoName = sanitizeForDockerTag(repoName);

//   // Example tag format: "repo-name-hash-of-sanitized-url"
//   const hashOfUrl = sanitizedUrl.split("").reduce((hash, char) => {
//     hash = ((hash << 5) - hash) + char.charCodeAt(0);
//     return hash & hash; // Convert to 32bit integer
//   }, 0).toString(16);

//   return `${sanitizedRepoName}:${hashOfUrl}`;
// }

// Example usage
// const gitUrl = "https://github.com/user/repo.git"; // Or "git@github.com:user/repo.git"
// const dockerTag = generateDockerImageTag(gitUrl);
// console.log(`Generated Docker image tag: ${dockerTag}`);

/**
 * The numeric GPU device indices in a container's HostConfig.DeviceRequests
 * (`--gpus "device=1,3"` is `DeviceIDs: ["1", "3"]`). Undefined when there are
 * none: no GPUs, or devices named by UUID, which the worker never allocates.
 */
export const gpuIndicesFromDeviceRequests = (
  deviceRequests: { DeviceIDs?: string[] | null }[] | null | undefined,
): number[] | undefined => {
  const indices = (deviceRequests ?? [])
    .flatMap((request) => request.DeviceIDs ?? [])
    .filter((id) => /^\d+$/.test(id))
    .map((id) => parseInt(id, 10));
  return indices.length > 0 ? indices : undefined;
};
