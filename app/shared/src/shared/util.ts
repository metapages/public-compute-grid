import {
  type DockerJobDefinitionInputRefs,
  DockerJobFinishedReason,
  type DockerJobImageBuild,
  DockerJobState,
  type EnqueueJob,
  type InMemoryDockerJob,
  type InputsRefs,
  type StateChangeValueFinished,
} from "@shared/types.ts";
import fetchRetry from "fetch-retry";
import { LRUMap } from "mnemonist";
import stringify from "safe-stable-stringify";

export const getJobStateString = (job?: InMemoryDockerJob | undefined | null): string => {
  if (!job) {
    return "__";
  }
  return job.state === DockerJobState.Finished ? `Finished(${job.finishedReason})` : `${job.state}`;
};

export const isFinishedStateWorthCaching = (reason: DockerJobFinishedReason): boolean => {
  switch (reason) {
    case DockerJobFinishedReason.Success:
    case DockerJobFinishedReason.TimedOut:
    case DockerJobFinishedReason.Error:
      return true;
    default:
      return false;
  }
};

export const isJobDeletedOrRemoved = (job?: InMemoryDockerJob | undefined | null): boolean => {
  return job?.state === DockerJobState.Removed || job?.finishedReason === DockerJobFinishedReason.Deleted;
};

export const isJobOkForSending = (job?: InMemoryDockerJob | undefined | null): boolean => {
  if (!job) {
    return false;
  }
  if (job.state === DockerJobState.Removed) {
    return false;
  }

  if (job.state === DockerJobState.Finished) {
    switch (job.finishedReason as DockerJobFinishedReason) {
      case DockerJobFinishedReason.WorkerLost:
      case DockerJobFinishedReason.JobReplacedByClient:
      case DockerJobFinishedReason.Cancelled:
      case DockerJobFinishedReason.Deleted:
        return false;
      default:
        break;
    }
  }

  return true;
};

export const setJobStateRunning = (
  job: InMemoryDockerJob,
  args: { worker: string; time: number },
): InMemoryDockerJob => {
  const { worker, time } = args;
  const newJob: InMemoryDockerJob = {
    ...job,
    state: DockerJobState.Running,
    worker,
    time,
  };
  delete newJob.finished;
  delete newJob.finishedReason;
  return newJob;
};

export const setJobStateQueued = (job: InMemoryDockerJob, args: { time: number }): InMemoryDockerJob => {
  const { time } = args;
  const newJob: InMemoryDockerJob = {
    ...job,
    state: DockerJobState.Queued,
    time,
  };
  delete newJob.finished;
  delete newJob.finishedReason;
  newJob.worker = "";
  return newJob;
};

export const setJobStateReQueued = (job: InMemoryDockerJob, args: { time: number }): InMemoryDockerJob => {
  const { time } = args;
  const newJob: InMemoryDockerJob = {
    ...job,
    state: DockerJobState.Queued,
    time,
  };
  delete newJob.finished;
  delete newJob.finishedReason;
  newJob.worker = "";
  return newJob;
};

export const setJobStateFinished = (
  job: InMemoryDockerJob,
  args: { finished: StateChangeValueFinished },
): InMemoryDockerJob => {
  const { finished } = args;
  const newJob: InMemoryDockerJob = {
    ...job,
    state: DockerJobState.Finished,
    finishedReason: finished.reason,
    time: finished.time,
    finished,
  };
  return newJob;
};

export const setJobStateRemoved = (
  job: InMemoryDockerJob,
): InMemoryDockerJob => {
  const newJob: InMemoryDockerJob = {
    ...job,
    state: DockerJobState.Removed,
  };
  delete newJob.finished;
  delete newJob.finishedReason;
  return newJob;
};

export const formatDefinitionS3Key = (id: string): string => {
  return `j/${id}/definition.json`;
};

export const formatResultsS3Key = (id: string): string => {
  return `j/${id}/result.json`;
};

export const formatLogsS3Key = (id: string, kind: "build" | "run"): string => {
  return `j/${id}/${kind}-logs.json`;
};

const resolvePreferredWorker = (workerA: string, workerB: string) => {
  return workerA.localeCompare(workerB) < 0 ? workerA : workerB;
};

// TODO make this more robust? 1. Replace invalid characters with underscores
export function sanitizeFilename(filename: string): string {
  // Replace invalid characters with underscores
  let sanitized = filename.replace(/[/\\:*?"<>|\0]/g, "_");

  // Limit character set (alphanumeric, underscore, hyphen, period)
  sanitized = sanitized.replace(/[^a-zA-Z0-9_\-.]/g, "_");

  // Remove leading periods
  sanitized = sanitized.replace(/^\.+/, "");

  // Trim whitespace
  sanitized = sanitized.trim();

  // Handle reserved filenames (Windows-specific)
  const reserved = [
    "con",
    "prn",
    "aux",
    "nul",
    "com1",
    "com2",
    "com3",
    "com4",
    "com5",
    "com6",
    "com7",
    "com8",
    "com9",
    "lpt1",
    "lpt2",
    "lpt3",
    "lpt4",
    "lpt5",
    "lpt6",
    "lpt7",
    "lpt8",
    "lpt9",
  ];
  if (reserved.includes(sanitized.split(".")[0])) {
    sanitized = sanitized + "_";
  }

  sanitized = sanitized.substring(0, 255);

  return sanitized;
}

/**
 * Cleans InputsRefs for hashing: for each DataRef, only includes `value` and `type`
 * (excludes `hash` and any other extra fields). For URL-type refs, normalizes the
 * URL to remove presigned parameters.
 *
 * NOTE: This takes ALL keys from the record - the set of input/configFile names
 * is unbounded and determined by the user's job configuration.
 */
const cleanInputsRefsForHash = (
  refs: InputsRefs | undefined,
): Record<string, { value: unknown; type?: string }> | undefined => {
  if (!refs) return undefined;
  const cleaned: Record<string, { value: unknown; type?: string }> = {};
  for (const key of Object.keys(refs)) {
    const ref = refs[key];
    let value = ref.value;
    // Normalize URLs to remove presigned/S3 query params
    if (ref.type === "url" && typeof value === "string") {
      value = reduceUrlToHashVersion(value);
    }
    const entry: { value: unknown; type?: string } = { value };
    if (ref.type != null) entry.type = ref.type;
    cleaned[key] = entry;
  }
  return cleaned;
};

/**
 * Params injected into a metaframe URL by an embedding parent page (metapage.io)
 * rather than by the user. They are plumbing, not container configuration, and
 * must never reach definition.env or the job hash: several of them vary per page
 * load or per viewer, which would give an otherwise identical container a new job
 * id on every refresh.
 *
 * `mp-` / `mp_` is the reserved prefix for these. Any NEW param metapage.io
 * injects must use it, and is then excluded here automatically with no change to
 * this repo. app.metapage.io has a test enforcing that
 * (frontend/browser/src/io/definition/tests/InjectedHashParamsConvention.test.ts).
 *
 * The set below is the frozen list of params that predate the convention. It
 * should never grow. Params the container itself owns (config, queueOverride,
 * ...) are handled separately by HashParamKeysSystem in the browser app.
 */
const ParentInjectedHashParamPrefix = /^mp[-_]/;

const ParentInjectedHashParamKeysLegacy = new Set([
  // deprecated spelling of mp-channel
  "channel",
  "CHANNEL",
  // injected only when the viewer does not own the metaframe, so it would
  // otherwise make job ids differ between viewers of the same metapage
  "readonly",
  "edit",
  "parent-metapage-id",
  "fs-path",
]);

export const isParentInjectedHashParamKey = (key: string): boolean =>
  ParentInjectedHashParamPrefix.test(key) || ParentInjectedHashParamKeysLegacy.has(key);

/**
 * Builds a deterministic hash blob from a DockerJobDefinitionInputRefs using only
 * whitelisted known fields. This ensures that extra fields added by clients, servers,
 * or intermediate systems do not affect the hash.
 *
 * Fields are grouped into:
 * 1. Scalar/array fields from DockerJobDefinitionInputsBase64V1 (fully known)
 * 2. Structured sub-objects with whitelisted sub-fields (build, requirements)
 * 3. Open-ended Record fields where all keys are included (env, files, inputs)
 *    - these are marked with comments since we can't whitelist individual keys
 */
const buildJobHashBlob = (job: DockerJobDefinitionInputRefs): Record<string, unknown> => {
  const blob: Record<string, unknown> = {};

  // --- Scalar/array fields (fully known from DockerJobDefinitionInputsBase64V1) ---
  // Use != null to exclude both undefined AND null, so that fields explicitly
  // set to null by external clients don't produce a different hash.
  if (job.v != null) blob.v = job.v;
  if (job.image != null) blob.image = job.image;
  if (job.command != null) blob.command = job.command;
  if (job.entrypoint != null) blob.entrypoint = job.entrypoint;
  if (job.workdir != null) blob.workdir = job.workdir;
  if (job.shmSize != null) blob.shmSize = job.shmSize;
  if (job.maxDuration != null) blob.maxDuration = job.maxDuration;
  if (job.tags != null) blob.tags = job.tags;

  // --- Structured sub-object: build (whitelisted sub-fields from DockerJobImageBuild) ---
  if (job.build) {
    const build: Partial<DockerJobImageBuild> = {};
    if (job.build.context != null) build.context = job.build.context;
    if (job.build.buildContext != null) build.buildContext = job.build.buildContext;
    if (job.build.filename != null) build.filename = job.build.filename;
    if (job.build.target != null) build.target = job.build.target;
    if (job.build.dockerfile != null) build.dockerfile = job.build.dockerfile;
    if (job.build.buildArgs != null) build.buildArgs = job.build.buildArgs;
    if (job.build.platform != null) build.platform = job.build.platform;
    if (Object.keys(build).length > 0) {
      blob.build = build;
    }
  }

  // --- Structured sub-object: requirements (whitelisted sub-fields) ---
  if (job.requirements) {
    const reqs: Record<string, unknown> = {};
    if (job.requirements.cpus != null) reqs.cpus = job.requirements.cpus;
    if (job.requirements.gpus != null) reqs.gpus = job.requirements.gpus;
    if (job.requirements.maxDuration != null) reqs.maxDuration = job.requirements.maxDuration;
    if (job.requirements.memory != null) reqs.memory = job.requirements.memory;
    if (Object.keys(reqs).length > 0) {
      blob.requirements = reqs;
    }
  }

  // --- Open-ended Record: env (all keys included, except parent-injected keys) ---
  // NOTE: env keys are unbounded - any hash param or user-defined env var can appear here.
  // We exclude keys injected by an embedding parent page (metapage.io), which change
  // every page refresh and would otherwise give the same container a new job id each load.
  if (job.env) {
    const env: Record<string, string> = {};
    for (const key of Object.keys(job.env)) {
      if (isParentInjectedHashParamKey(key)) continue;
      env[key] = job.env[key];
    }
    if (Object.keys(env).length > 0) {
      blob.env = env;
    }
  }

  // --- Open-ended Record: files (all keys included, DataRef cleaned) ---
  // NOTE: file names are unbounded - determined by the user's job configuration.
  const cleanedFiles = cleanInputsRefsForHash(job.files);
  if (cleanedFiles && Object.keys(cleanedFiles).length > 0) {
    blob.files = cleanedFiles;
  }

  // --- Open-ended Record: inputs (all keys included, DataRef cleaned) ---
  // NOTE: input names are unbounded - determined by the user's metaframe inputs.
  const cleanedInputs = cleanInputsRefsForHash(job.inputs);
  if (cleanedInputs && Object.keys(cleanedInputs).length > 0) {
    blob.inputs = cleanedInputs;
  }

  return blob;
};

export const shaDockerJob = (
  job: DockerJobDefinitionInputRefs,
): Promise<string> => {
  if (!job) {
    throw new Error("shaDockerJob: job is undefined");
  }
  const blob = buildJobHashBlob(job);
  return shaObject(blob);
};

const SHA256 = /^[0-9a-f]{64}$/;
const JOB_NAME = /^[A-Za-z0-9_-]{1,128}$/;

/** A sha256 in a job URL is a content address: it only ever means that content. */
export const isSha256 = (value: string): boolean => SHA256.test(value);

/** Names are safe in a URL path and a store key, and cannot be mistaken for a sha256. */
export const isJobName = (value: string): boolean => JOB_NAME.test(value) && !/^[0-9a-fA-F]{64}$/.test(value);

/** A submission's `jobId` that cannot be accepted. Clients get a 400. */
export class InvalidJobIdError extends Error {
  override name = "InvalidJobIdError";
}

/** sha256 of a run's inputs: the `i` in `/j/<job>?i=<inputs>` */
export const shaInputs = (inputs: InputsRefs): Promise<string> => {
  return shaObject(cleanInputsRefsForHash(inputs));
};

/**
 * The definition a run actually executes: its job's definition with the run's
 * inputs folded in. This is what is stored under the run id and what workers
 * receive, so a run looks like any other job to the queue and the workers.
 */
export const mergeRunInputs = (
  job: Pick<EnqueueJob, "definition" | "inputs">,
): DockerJobDefinitionInputRefs => {
  if (!job.inputs || Object.keys(job.inputs).length === 0) {
    return job.definition;
  }
  return { ...job.definition, inputs: { ...job.definition.inputs, ...job.inputs } };
};

/**
 * Derives a submission's ids. Safe to call on an already prepared job.
 *
 *   jobHash    = sha256(definition)
 *   jobId      = the client's name for the job, else jobHash
 *   inputsHash = sha256(inputs)
 *   id         = sha256(definition + inputs), the run
 *
 * Results are keyed by content, not by jobId, so publishing a new version under
 * the same name runs it rather than answering with the old version's results.
 *
 * Every id is derived here, never taken from the client: they decide what a
 * content address points at (see DB.queueJobRecordIdentity). Likewise a `jobId` is
 * either the hash of this definition or a name, otherwise anyone could publish
 * other content at a job's hash.
 *
 * Inputs inside the definition are a run's inputs, not part of the job: that is
 * how a stored run holds them (see mergeRunInputs).
 */
export const prepareEnqueueJob = async (
  job: Omit<EnqueueJob, "id"> & { id?: string },
): Promise<EnqueueJob> => {
  const { inputs: runInputs, jobHash: _jobHash, inputsHash: _inputsHash, ...rest } = job;
  const { inputs: definitionInputs, ...definition } = job.definition;
  const inputs = { ...definitionInputs, ...runInputs };
  const hasInputs = Object.keys(inputs).length > 0;
  const jobHash = await shaDockerJob(definition);
  if (job.jobId && job.jobId !== jobHash && !isJobName(job.jobId)) {
    throw new InvalidJobIdError(
      `Invalid jobId "${job.jobId}": it must be the sha256 of this job's definition, ` +
        `or a name of 1-128 characters from A-Z a-z 0-9 _ - (and not itself shaped like a sha256)`,
    );
  }
  const prepared: EnqueueJob = {
    ...rest,
    definition,
    id: await shaDockerJob(mergeRunInputs({ definition, inputs })),
    jobId: job.jobId || jobHash,
    jobHash,
  };
  if (hasInputs) {
    prepared.inputs = inputs;
    prepared.inputsHash = await shaInputs(inputs);
  }
  return prepared;
};

/**
 * The canonical URL path of a job or a run:
 *
 *   /j/<sha256 job>
 *   /j/<sha256 job>?i=<sha256 inputs>
 *   /j/<named job>?v=<sha256 job>&i=<sha256 inputs>
 */
export const formatJobUrlPath = (
  job: { jobId?: string; jobHash?: string; inputsHash?: string },
): string => {
  const params: string[] = [];
  if (job.jobHash && job.jobHash !== job.jobId) params.push(`v=${job.jobHash}`);
  if (job.inputsHash) params.push(`i=${job.inputsHash}`);
  return `/j/${job.jobId}${params.length ? `?${params.join("&")}` : ""}`;
};

/** A client-chosen name in a response header: it can hold anything, including quotes and newlines. */
export const formatContentDisposition = (filename: string): string => {
  const name = filename.split("/").pop() ?? filename;
  const ascii = name.replace(/[^\x20-\x7e]|["\\]/g, "_");
  return `attachment; filename="${ascii}"; filename*=UTF-8''${encodeURIComponent(name)}`;
};

const reduceUrlToHashVersion = (url: string): string => {
  if (url.includes("/presignedurl/")) {
    const tokens = url.split("/presignedurl/");
    return tokens[0];
  }
  if (
    url.startsWith("https://metaframe-asman-test.s3.us-west-1.amazonaws.com")
  ) {
    const urlBlob = new URL(url);
    urlBlob.search = "";
    urlBlob.hash = "";
    return urlBlob.href;
  }

  return url;
};

export const shaObject = (obj: unknown): Promise<string> => {
  const orderedStringFromObject = stringify(obj);
  const msgBuffer = new TextEncoder().encode(orderedStringFromObject);
  return sha256Buffer(msgBuffer);
};

export const sha256Buffer = async (buffer: BufferSource): Promise<string> => {
  const hashBuffer = await crypto.subtle.digest("SHA-256", buffer);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  const hashHex = hashArray.map((b) => b.toString(16).padStart(2, "0")).join(
    "",
  );
  return hashHex;
};

export const fetchRobust: ReturnType<typeof fetchRetry> = fetchRetry(fetch, {
  retries: 5,
  // eslint-disable-next-line
  retryDelay: (
    attempt: number,
    _error: unknown,
    _response: Response | null,
  ) => {
    return Math.pow(2, attempt) * 100; //
  },

  retryOn: (attempt: number, error: unknown, response: Response | null) => {
    // retry on any network error, or 4xx or 5xx status codes
    if (error !== null || (response && response.status >= 400)) {
      if (attempt > 7) {
        if (error) {
          console.error(error);
        }
        console.log(
          `Retried too many times: response.status=${response?.status} response.statusText=${response?.statusText} attempt number ${
            attempt + 1
          } url=${response?.url}`,
        );
        return false;
      }
      return true;
    }
    return false;
  },
});

/**
 * The situation here is fluid and dynamic, workers and servers and clients coming
 * and going all the time. The db is the source of truth, but we
 * resolve conflicts and differences as they come in, and allow jobs to be requeued.
 * This means that resolving which of two jobs is the *most correct* is critical
 * and drives a lot of the rest of the dynamics.
 * At a high level:
 *  - if a job is Finished, it trumps most things
 *  - if two jobs seem the same, the one queued first is priority
 *  - other conflicts: check the time, the earliest wins
 *  - otherwise, whoever has the longest history is priority
 */
export const resolveMostCorrectJob = (
  // jobA is the DEFAULT, if that matters
  jobA: InMemoryDockerJob,
  jobB: InMemoryDockerJob,
): InMemoryDockerJob => {
  if (jobA && !jobB) {
    return jobA;
  }

  if (!jobA && jobB) {
    return jobB;
  }

  switch (jobA.state) {
    case DockerJobState.Queued:
      switch (jobB.state) {
        case DockerJobState.Queued:
          return jobA.time < jobB.time ? jobA : jobB;
        case DockerJobState.Running:
          return jobB;
        case DockerJobState.Finished:
          switch (jobB.finishedReason as DockerJobFinishedReason) {
            case DockerJobFinishedReason.Success:
              return jobB;
            case DockerJobFinishedReason.TimedOut:
              return jobB;
            case DockerJobFinishedReason.Error:
              return jobB;
            case DockerJobFinishedReason.WorkerLost:
              return jobA;
            case DockerJobFinishedReason.JobReplacedByClient:
              return jobB;
            case DockerJobFinishedReason.Cancelled:
              // if jobA was queued AFTER jobB was cancelled, then jobA is the correct one
              return jobA.time > jobB.time ? jobA : jobB;
            case DockerJobFinishedReason.Deleted:
              return jobA;
            default:
              return jobA;
          }
        case DockerJobState.Removed:
          return jobA;
        default:
          return jobA;
      }
    case DockerJobState.Running:
      switch (jobB.state) {
        case DockerJobState.Running: {
          if (jobA.worker === jobB.worker) {
            return jobA;
          }

          const workerA = jobA.worker;
          const workerB = jobB.worker;

          const preferredWorker = resolvePreferredWorker(
            workerA,
            workerB,
          );
          if (preferredWorker === workerA) {
            return jobA;
          } else {
            return jobB;
          }
        }
        case DockerJobState.Finished:
          return jobB;
        case DockerJobState.Removed:
          return jobA; // Running -> Finished -> Removed, Removed cannot skip over Finished
        default:
          return jobA;
      }

    case DockerJobState.Finished:
      switch (jobB.state) {
        case DockerJobState.Queued:
          switch (jobA.finishedReason as DockerJobFinishedReason) {
            case DockerJobFinishedReason.Success:
              return jobA;
            case DockerJobFinishedReason.TimedOut:
              return jobA;
            case DockerJobFinishedReason.Error:
              return jobA;
            case DockerJobFinishedReason.WorkerLost:
              return jobB;
            case DockerJobFinishedReason.JobReplacedByClient:
              return jobA;
            case DockerJobFinishedReason.Cancelled:
              // if jobB was queued AFTER jobA was cancelled, then jobB is the correct one
              return jobB.time > jobA.time ? jobB : jobA;
            case DockerJobFinishedReason.Deleted:
              return jobB;
            default:
              return jobB;
          }
        case DockerJobState.Running:
          return jobA;

        case DockerJobState.Finished:
          switch (jobA.finishedReason as DockerJobFinishedReason) {
            case DockerJobFinishedReason.Success:
              switch (jobB.finishedReason as DockerJobFinishedReason) {
                case DockerJobFinishedReason.Success:
                  return jobA.time < jobB.time ? jobA : jobB;
                case DockerJobFinishedReason.TimedOut:
                  return jobA;
                case DockerJobFinishedReason.Error:
                  return jobA;
                case DockerJobFinishedReason.WorkerLost:
                  return jobA;
                case DockerJobFinishedReason.JobReplacedByClient:
                  return jobA;
                case DockerJobFinishedReason.Cancelled:
                  // it has already been cancelled.
                  return jobB;
                case DockerJobFinishedReason.Deleted:
                  return jobB;
                default:
                  return jobA;
              }

            case DockerJobFinishedReason.TimedOut:
              switch (jobB.finishedReason as DockerJobFinishedReason) {
                case DockerJobFinishedReason.Success:
                  return jobB;
                case DockerJobFinishedReason.TimedOut:
                  return jobA.time < jobB.time ? jobA : jobB;
                case DockerJobFinishedReason.Error:
                  return jobB;
                case DockerJobFinishedReason.WorkerLost:
                  return jobB;
                case DockerJobFinishedReason.JobReplacedByClient:
                  return jobA;
                case DockerJobFinishedReason.Cancelled:
                  return jobA;
                case DockerJobFinishedReason.Deleted:
                  return jobB;
                default:
                  return jobA;
              }
            case DockerJobFinishedReason.Error:
              switch (jobB.finishedReason as DockerJobFinishedReason) {
                case DockerJobFinishedReason.Success:
                  return jobB;
                case DockerJobFinishedReason.TimedOut:
                  return jobA;
                case DockerJobFinishedReason.Error:
                  return jobA.time < jobB.time ? jobA : jobB;
                case DockerJobFinishedReason.WorkerLost:
                  return jobA;
                case DockerJobFinishedReason.JobReplacedByClient:
                  return jobA;
                case DockerJobFinishedReason.Cancelled:
                  return jobA;
                case DockerJobFinishedReason.Deleted:
                  return jobB;
                default:
                  return jobA;
              }

            case DockerJobFinishedReason.WorkerLost:
              return jobB;

            case DockerJobFinishedReason.JobReplacedByClient:
              switch (jobB.finishedReason as DockerJobFinishedReason) {
                case DockerJobFinishedReason.Success:
                  return jobB;
                case DockerJobFinishedReason.TimedOut:
                  return jobB;
                case DockerJobFinishedReason.Error:
                  return jobB;
                case DockerJobFinishedReason.WorkerLost:
                  return jobA;
                case DockerJobFinishedReason.JobReplacedByClient:
                  return jobA;
                case DockerJobFinishedReason.Cancelled:
                  return jobA;
                case DockerJobFinishedReason.Deleted:
                  return jobB;
                default:
                  return jobB;
              }
            case DockerJobFinishedReason.Cancelled:
              switch (jobB.finishedReason as DockerJobFinishedReason) {
                case DockerJobFinishedReason.Success:
                  // it has already been cancelled.
                  return jobA;
                case DockerJobFinishedReason.TimedOut:
                  return jobB;
                case DockerJobFinishedReason.Error:
                  return jobB;
                case DockerJobFinishedReason.WorkerLost:
                  return jobA;
                case DockerJobFinishedReason.JobReplacedByClient:
                  return jobA;
                case DockerJobFinishedReason.Cancelled:
                  return jobA;
                case DockerJobFinishedReason.Deleted:
                  return jobB;
                default:
                  return jobB;
              }

            case DockerJobFinishedReason.Deleted:
              switch (jobB.finishedReason as DockerJobFinishedReason) {
                case DockerJobFinishedReason.Success:
                  return jobA;
                case DockerJobFinishedReason.TimedOut:
                  return jobA;
                case DockerJobFinishedReason.Error:
                  return jobA;
                case DockerJobFinishedReason.WorkerLost:
                  return jobA;
                case DockerJobFinishedReason.JobReplacedByClient:
                  return jobA;
                case DockerJobFinishedReason.Cancelled:
                  return jobA;
                case DockerJobFinishedReason.Deleted:
                  return jobA;
                default:
                  return jobB;
              }
            default:
              return jobB;
          }

        case DockerJobState.Removed:
          return jobB; // Finished -> Removed
        default:
          return jobA;
      }

    case DockerJobState.Removed:
      switch (jobB.state) {
        case DockerJobState.Queued:
          return jobB;
        case DockerJobState.Running:
          return jobB;
        case DockerJobState.Finished:
          return jobA;
        case DockerJobState.Removed:
          return jobA.time < jobB.time ? jobA : jobB;
        default:
          return jobA;
      }
    default:
      return jobA;
  }
};

const jobColorCache = new LRUMap<string, string>(1000);

/**
 * Creates a colorized console.log string deterministically based on jobId.
 * Uses ANSI color codes to provide consistent color coding for job-related logs.
 * @param jobId - The job identifier
 * @returns A string with ANSI color codes that can be used in console.log
 */
export const getJobColorizedString = (jobId: string): string => {
  if (!jobId) {
    return jobId;
  }
  const cachedColor = jobColorCache.get(jobId);
  if (cachedColor) {
    return cachedColor;
  }

  // Generate a hash from the jobId to ensure deterministic color selection
  let hash = 0;
  for (let i = 0; i < jobId.length; i++) {
    const char = jobId.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32-bit integer
  }

  // Use the hash to select from a predefined set of colors and styles
  const colorStyles = [
    "\x1b[31m", // Red
    "\x1b[32m", // Green
    "\x1b[33m", // Yellow
    "\x1b[34m", // Blue
    "\x1b[35m", // Magenta
    "\x1b[36m", // Cyan
    "\x1b[91m", // Bright Red
    "\x1b[92m", // Bright Green
    "\x1b[93m", // Bright Yellow
    "\x1b[94m", // Bright Blue
    "\x1b[95m", // Bright Magenta
    "\x1b[96m", // Bright Cyan
    "\x1b[1;31m", // Bold Red
    "\x1b[1;32m", // Bold Green
    "\x1b[1;33m", // Bold Yellow
    "\x1b[1;34m", // Bold Blue
    "\x1b[1;35m", // Bold Magenta
    "\x1b[1;36m", // Bold Cyan
    "\x1b[1;91m", // Bold Bright Red
    "\x1b[1;92m", // Bold Bright Green
    "\x1b[1;93m", // Bold Bright Yellow
    "\x1b[1;94m", // Bold Bright Blue
    "\x1b[1;95m", // Bold Bright Magenta
    "\x1b[1;96m", // Bold Bright Cyan
    // "\x1b[4;31m", // Underline Red
    // "\x1b[4;32m", // Underline Green
    // "\x1b[4;33m", // Underline Yellow
    // "\x1b[4;34m", // Underline Blue
    // "\x1b[4;35m", // Underline Magenta
    // "\x1b[4;36m", // Underline Cyan
    // "\x1b[4;91m", // Underline Bright Red
    // "\x1b[4;92m", // Underline Bright Green
    // "\x1b[4;93m", // Underline Bright Yellow
    // "\x1b[4;94m", // Underline Bright Blue
    // "\x1b[4;95m", // Underline Bright Magenta
    // "\x1b[4;96m", // Underline Bright Cyan
    // "\x1b[7;31m", // Reverse Red
    // "\x1b[7;32m", // Reverse Green
    // "\x1b[7;33m", // Reverse Yellow
    // "\x1b[7;34m", // Reverse Blue
    // "\x1b[7;35m", // Reverse Magenta
    // "\x1b[7;36m", // Reverse Cyan
    // "\x1b[7;91m", // Reverse Bright Red
    // "\x1b[7;92m", // Reverse Bright Green
    // "\x1b[7;93m", // Reverse Bright Yellow
    // "\x1b[7;94m", // Reverse Bright Blue
    // "\x1b[7;95m", // Reverse Bright Magenta
    // "\x1b[7;96m", // Reverse Bright Cyan
  ];

  const colorIndex = Math.abs(hash) % colorStyles.length;
  const selectedColor = colorStyles[colorIndex];
  const resetColor = "\x1b[0m";

  const s = `${selectedColor}[${jobId.substring(0, 6)}]${resetColor}`;
  jobColorCache.set(jobId, s);

  return s;
};

const queueColorCache = new LRUMap<string, string>(1000);

/**
 * Creates a colorized console.log string deterministically based on jobId.
 * Uses ANSI color codes to provide consistent color coding for job-related logs.
 * @param jobId - The job identifier
 * @returns A string with ANSI color codes that can be used in console.log
 */
export const getQueueColorizedString = (queue: string): string => {
  if (!queue) {
    return queue;
  }
  const cachedColor = queueColorCache.get(queue);
  if (cachedColor) {
    return cachedColor;
  }

  // Generate a hash from the jobId to ensure deterministic color selection
  let hash = 0;
  for (let i = 0; i < queue.length; i++) {
    const char = queue.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32-bit integer
  }

  // Use the hash to select from a predefined set of colors and styles
  const colorStyles = [
    // "\x1b[31m", // Red
    // "\x1b[32m", // Green
    // "\x1b[33m", // Yellow
    // "\x1b[34m", // Blue
    // "\x1b[35m", // Magenta
    // "\x1b[36m", // Cyan
    // "\x1b[91m", // Bright Red
    // "\x1b[92m", // Bright Green
    // "\x1b[93m", // Bright Yellow
    // "\x1b[94m", // Bright Blue
    // "\x1b[95m", // Bright Magenta
    // "\x1b[96m", // Bright Cyan
    // "\x1b[1;31m", // Bold Red
    // "\x1b[1;32m", // Bold Green
    // "\x1b[1;33m", // Bold Yellow
    // "\x1b[1;34m", // Bold Blue
    // "\x1b[1;35m", // Bold Magenta
    // "\x1b[1;36m", // Bold Cyan
    // "\x1b[1;91m", // Bold Bright Red
    // "\x1b[1;92m", // Bold Bright Green
    // "\x1b[1;93m", // Bold Bright Yellow
    // "\x1b[1;94m", // Bold Bright Blue
    // "\x1b[1;95m", // Bold Bright Magenta
    // "\x1b[1;96m", // Bold Bright Cyan
    // "\x1b[4;31m", // Underline Red
    // "\x1b[4;32m", // Underline Green
    // "\x1b[4;33m", // Underline Yellow
    // "\x1b[4;34m", // Underline Blue
    // "\x1b[4;35m", // Underline Magenta
    // "\x1b[4;36m", // Underline Cyan
    // "\x1b[4;91m", // Underline Bright Red
    // "\x1b[4;92m", // Underline Bright Green
    // "\x1b[4;93m", // Underline Bright Yellow
    // "\x1b[4;94m", // Underline Bright Blue
    // "\x1b[4;95m", // Underline Bright Magenta
    // "\x1b[4;96m", // Underline Bright Cyan
    "\x1b[7;31m", // Reverse Red
    "\x1b[7;32m", // Reverse Green
    "\x1b[7;33m", // Reverse Yellow
    "\x1b[7;34m", // Reverse Blue
    "\x1b[7;35m", // Reverse Magenta
    "\x1b[7;36m", // Reverse Cyan
    "\x1b[7;91m", // Reverse Bright Red
    "\x1b[7;92m", // Reverse Bright Green
    "\x1b[7;93m", // Reverse Bright Yellow
    "\x1b[7;94m", // Reverse Bright Blue
    "\x1b[7;95m", // Reverse Bright Magenta
    "\x1b[7;96m", // Reverse Bright Cyan
  ];

  const colorIndex = Math.abs(hash) % colorStyles.length;
  const selectedColor = colorStyles[colorIndex];
  const resetColor = "\x1b[0m";

  const s = `${selectedColor}[[${queue.substring(0, 14)}]]${resetColor}`;
  queueColorCache.set(queue, s);

  return s;
};

const workerColorCache = new LRUMap<string, string>(1000);

export const getWorkerColorizedString = (worker: string): string => {
  if (!worker) {
    return worker;
  }
  const cachedColor = workerColorCache.get(worker);
  if (cachedColor) {
    return cachedColor;
  }

  // Generate a hash from the jobId to ensure deterministic color selection
  let hash = 0;
  for (let i = 0; i < worker.length; i++) {
    const char = worker.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32-bit integer
  }

  // Use the hash to select from a predefined set of colors and styles
  const colorStyles = [
    // "\x1b[31m", // Red
    // "\x1b[32m", // Green
    // "\x1b[33m", // Yellow
    // "\x1b[34m", // Blue
    // "\x1b[35m", // Magenta
    // "\x1b[36m", // Cyan
    // "\x1b[91m", // Bright Red
    // "\x1b[92m", // Bright Green
    // "\x1b[93m", // Bright Yellow
    // "\x1b[94m", // Bright Blue
    // "\x1b[95m", // Bright Magenta
    // "\x1b[96m", // Bright Cyan
    // "\x1b[1;31m", // Bold Red
    // "\x1b[1;32m", // Bold Green
    // "\x1b[1;33m", // Bold Yellow
    // "\x1b[1;34m", // Bold Blue
    // "\x1b[1;35m", // Bold Magenta
    // "\x1b[1;36m", // Bold Cyan
    // "\x1b[1;91m", // Bold Bright Red
    // "\x1b[1;92m", // Bold Bright Green
    // "\x1b[1;93m", // Bold Bright Yellow
    // "\x1b[1;94m", // Bold Bright Blue
    // "\x1b[1;95m", // Bold Bright Magenta
    // "\x1b[1;96m", // Bold Bright Cyan
    "\x1b[4;31m", // Underline Red
    "\x1b[4;32m", // Underline Green
    "\x1b[4;33m", // Underline Yellow
    "\x1b[4;34m", // Underline Blue
    "\x1b[4;35m", // Underline Magenta
    "\x1b[4;36m", // Underline Cyan
    "\x1b[4;91m", // Underline Bright Red
    "\x1b[4;92m", // Underline Bright Green
    "\x1b[4;93m", // Underline Bright Yellow
    "\x1b[4;94m", // Underline Bright Blue
    "\x1b[4;95m", // Underline Bright Magenta
    "\x1b[4;96m", // Underline Bright Cyan
    // "\x1b[7;31m", // Reverse Red
    // "\x1b[7;32m", // Reverse Green
    // "\x1b[7;33m", // Reverse Yellow
    // "\x1b[7;34m", // Reverse Blue
    // "\x1b[7;35m", // Reverse Magenta
    // "\x1b[7;36m", // Reverse Cyan
    // "\x1b[7;91m", // Reverse Bright Red
    // "\x1b[7;92m", // Reverse Bright Green
    // "\x1b[7;93m", // Reverse Bright Yellow
    // "\x1b[7;94m", // Reverse Bright Blue
    // "\x1b[7;95m", // Reverse Bright Magenta
    // "\x1b[7;96m", // Reverse Bright Cyan
  ];

  const colorIndex = Math.abs(hash) % colorStyles.length;
  const selectedColor = colorStyles[colorIndex];
  const resetColor = "\x1b[0m";

  const s = `${selectedColor}~~${worker.substring(0, 14)}~~${resetColor}`;
  workerColorCache.set(worker, s);

  return s;
};
