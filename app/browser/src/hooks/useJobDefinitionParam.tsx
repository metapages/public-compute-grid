/**
 * The job definition that drives the app comes from one of two URL forms:
 *
 *   1. Embedded (long): `/#?job=<base64-json>&queue=<q>` — the whole definition
 *      lives in the hash. This is the editable, self-contained form.
 *   2. Short:           `/j/<jobId>#?queue=<q>` — only the sha256 id is in the
 *      path; the definition is fetched from the server by id at boot (see
 *      hydrateShortJobFromPath, called from index.tsx) and stashed here.
 *
 * `useJobDefinitionParam()` is a drop-in for `useHashParamJson("job")` that
 * reads from whichever form is active. The MCP server's `submit_job` returns the
 * short form (paste-friendly); editing reverts to the embedded form so the job
 * stays self-contained and tweakable — mirroring metaframes/metaframe-js.
 */
import { useCallback } from "react";

import { create } from "zustand";

import { DockerJobDefinitionParamsInUrlHash } from "/@shared/client";

import { setHashParamValueJsonInUrl } from "@metapages/hash-query";
import { useHashParamJson } from "@metapages/hash-query/react-hooks";

interface ShortJobStore {
  // The sha256 job id from a `/j/<jobId>` path, or undefined when not in short
  // URL mode. Its presence is what flips reads/writes into short-URL behaviour.
  shortJobId: string | undefined;
  shortJobDefinition: DockerJobDefinitionParamsInUrlHash | undefined;
  setShortJob: (
    shortJobId: string | undefined,
    shortJobDefinition: DockerJobDefinitionParamsInUrlHash | undefined,
  ) => void;
}

export const useShortJobStore = create<ShortJobStore>(set => ({
  shortJobId: undefined,
  shortJobDefinition: undefined,
  setShortJob: (shortJobId, shortJobDefinition) => set({ shortJobId, shortJobDefinition }),
}));

// Path form: /j/<jobId> (a trailing .json — the JSON state endpoint — is ignored
// here; that's not an SPA load).
const SHORT_JOB_PATH = /^\/j\/([^/]+?)(?:\.json)?\/?$/;

export const getShortJobIdFromPath = (): string | undefined => {
  const match = SHORT_JOB_PATH.exec(window.location.pathname);
  return match ? match[1] : undefined;
};

/**
 * If the page was opened at `/j/<jobId>`, fetch the persisted definition and
 * stash it in the store. Called once at boot, before React renders, so the
 * first `useDockerJobDefinition` pass already sees the definition.
 * Resolves true when a short job was loaded.
 */
export const hydrateShortJobFromPath = async (): Promise<boolean> => {
  const jobId = getShortJobIdFromPath();
  if (!jobId) {
    return false;
  }
  try {
    const resp = await fetch(`${window.location.origin}/j/${jobId}/definition.json`);
    if (!resp.ok) {
      return false;
    }
    const json = await resp.json();
    const definition = json?.data as DockerJobDefinitionParamsInUrlHash | undefined;
    if (!definition) {
      return false;
    }
    useShortJobStore.getState().setShortJob(jobId, definition);
    return true;
  } catch (_err) {
    // Job not found / network error: fall back to the empty app state.
    return false;
  }
};

export const useJobDefinitionParam = (): [
  DockerJobDefinitionParamsInUrlHash | undefined,
  (next: DockerJobDefinitionParamsInUrlHash | undefined) => void,
] => {
  const [hashJob, setHashJob] = useHashParamJson<DockerJobDefinitionParamsInUrlHash | undefined>("job");
  const shortJobId = useShortJobStore(s => s.shortJobId);
  const shortJobDefinition = useShortJobStore(s => s.shortJobDefinition);
  const setShortJob = useShortJobStore(s => s.setShortJob);

  const value = shortJobId ? shortJobDefinition : hashJob;

  const setValue = useCallback(
    (next: DockerJobDefinitionParamsInUrlHash | undefined) => {
      if (shortJobId) {
        // Editing exits short-URL mode. Rebuild the self-contained embedded form
        // at the base path: start from the current hash (preserving queue and
        // any other params) and write the edited definition into `job`. This is
        // a real navigation (reload) — same as the metaframe-js reference does
        // when the user starts editing a short URL.
        const baseUrl = new URL(`${window.location.origin}/${window.location.hash}`);
        const url = next ? setHashParamValueJsonInUrl(baseUrl, "job", next) : baseUrl;
        setShortJob(undefined, undefined);
        window.location.href = url.toString();
        return;
      }
      setHashJob(next);
    },
    [shortJobId, setHashJob, setShortJob],
  );

  return [value, setValue];
};
