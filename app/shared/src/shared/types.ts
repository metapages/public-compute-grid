export type IsStdErr = boolean;
export type ConsoleLogLine = [string, number, IsStdErr] | [string, number];

export type JobInputs = { [key: string]: string };

// represents a way of getting a blob of data (inputs/outputs)
export enum DataRefType {
  base64 = "base64", //default, value is a base64 encoded bytes
  url = "url", // request the data at this URL
  utf8 = "utf8",
  json = "json",
  // the internal system can get this data blob given the key address (stored in the value)
  // this is typically the sha256 hash of the data
  key = "key",
}

const DataRefTypeKeys: string[] = Object.keys(DataRefType).filter((key) => isNaN(Number(key)));
export const DataRefTypesSet: Set<string> = new Set(DataRefTypeKeys);
export const DataRefTypeDefault = DataRefType.utf8;

export type DataRef<T = string> = {
  value: T;
  type?: DataRefType;
  hash?: string;
};

export const isDataRef = (value: unknown): boolean => {
  return !!(
    value &&
    typeof value === "object" &&
    (value as DataRef)?.type &&
    DataRefTypesSet.has((value as DataRef).type!) &&
    (value as DataRef)?.value
  );
};

export type Image = string;
export type Command = string;
export type Env = { [name in string]: string } | undefined;
export type InputsRefs = { [name in string]: DataRef };
// values are base64 encoded buffers
export type InputsBase64String = { [name in string]: string };

export type DockerJobImageBuild = {
  context?: string;
  buildContext?: string;
  filename?: string;
  target?: string;
  // An actual Dockerfile content
  dockerfile?: string;
  buildArgs?: string[];
  // https://docs.docker.com/reference/cli/docker/buildx/build/#platform
  platform?: string;
};

// inputs values are base64 encoded strings
export type DockerJobDefinitionInputsBase64V1 = {
  // version, incrementing
  v?: number;
  // the docker image OR git repository URL
  image?: Image;
  // docker image build configuration
  build?: DockerJobImageBuild;
  // docker command
  command?: Command;
  // docker env vars, not currently implemented on the client
  env?: Env;
  // entrypoint?: string[];
  entrypoint?: string;
  workdir?: string;
  // https://docs.docker.com/engine/containers/run/#user-memory-constraints
  shmSize?: string;

  // Job files, part of the job sha. Keys are paths in the container: absolute
  // paths are used as they are, relative paths are under the working directory.
  files?: InputsBase64String;
  // A run's EnqueueJob.inputs, written to /inputs. Only set on a stored run (see
  // mergeRunInputs): submitted here they are moved to EnqueueJob.inputs.
  inputs?: InputsBase64String;
  // eg "1h", "20m", "10s"
  maxDuration?: string;
  // gpu?: boolean;
  // these restrict to workers with the same tags
  tags?: string[];

  requirements?: {
    cpus?: number;
    gpus?: number;
    maxDuration?: string;
    memory?: string;
  };
};

export const DefaultNamespace = "_";

// as soon as the DockerJobDefinition hits the server, it is converted
// immediately to this version, otherwise big lumps in the inputs will
// completely clog up the data pipes. Stay small out there, definitions,
// you're the living entities flowing
export type DockerJobDefinitionInputRefs =
  & Omit<
    DockerJobDefinitionInputsBase64V1,
    "files" | "inputs"
  >
  & {
    files?: InputsRefs;
    inputs?: InputsRefs;
  };

export interface DockerRunResult {
  StatusCode?: number;
  logs: ConsoleLogLine[];
  duration?: number;
  error?: unknown;
  isTimedOut: boolean;
}

export interface DockerRunResultWithOutputs extends DockerRunResult {
  outputs: InputsRefs;
}

/**
 * Think very hard and carefully before adding a new state.
 * There is a lot of power in keeping this part simple.
 */
export enum DockerJobState {
  Queued = "Queued",
  Running = "Running",
  Finished = "Finished",
  // Placeholder, to remove ambiguity so it's clear
  // it's not simply not loaded from the db, rather it's been removed
  // from the queue. This state stays for a few seconds to ensure state
  // is correctly propagated, then is simply deleted
  Removed = "Removed",
}

/**
 * Add as many as needed to help anyone understand why a job finished
 */
export enum DockerJobFinishedReason {
  Cancelled = "Cancelled",
  Error = "Error",
  JobReplacedByClient = "JobReplacedByClient",
  Success = "Success",
  TimedOut = "TimedOut",
  WorkerLost = "WorkerLost",
  Deleted = "Deleted",
}

// TODO: this tells us that we need two finished states
export const isFinalDockerJobFinishedReason = (reason?: DockerJobFinishedReason): boolean => {
  return reason === DockerJobFinishedReason.Error ||
    reason === DockerJobFinishedReason.Success ||
    reason === DockerJobFinishedReason.TimedOut;
};

export type DockerJobStateValue =
  | StateChangeValueQueued
  | StateChangeValueRunning
  | StateChangeValueFinished;

export interface StateChange {
  // 'id' implies permanence, and you should never delete it
  // 'tag' can be changed, when a driver for the state machine
  // updates or takes over another processes control of the fsm process
  tag: string;
  state: DockerJobState;
  job: string;
  value: DockerJobStateValue;
}

export type DockerJobControlConfig = {
  // namespace is a string that is used to identify the user/client
  // there can only be ONE job per userspace. it's like the user
  // plus document, or whatever is needed to uniquely limit a job
  // to a single userspace. It's not quite just a user since users
  // can run multiple jobs at once, but it's a subspace of a queue
  // that only tolerates a single job, all previous jobs in that
  // namespace are removed (but only killed if no-one is also running that job)
  namespace?: string;
  callbacks?: {
    queued?: {
      url: string;
      payload?: unknown;
    };
    finished?: {
      url: string;
      payload?: unknown;
    };
  };
  // if the job is not finished within this time, it is killed
  maxDuration?: string;
  outputs?: {
    nhost?: {
      PAT: string;
      path: string;
    };
  };
};

export interface StateChangeValue {
  type: DockerJobState;
  time: number;
}

/**
 * This state change contains the job definition.
 * This means history is recoverable.
 */
export interface StateChangeValueQueued extends StateChangeValue {
  enqueued: EnqueueJob;
}

export interface StateChangeValueRunning extends StateChangeValue {
  worker: string;
}

export interface StateChangeValueFinished extends StateChangeValue {
  result?: DockerRunResultWithOutputs;
  reason: DockerJobFinishedReason;
  message?: string;
  worker?: string;
  namespace?: string;
}

export interface InMemoryDockerJob {
  queuedTime: number;
  debug?: boolean;
  state: DockerJobState;
  time: number;
  worker: string; // blank means no worker
  finished?: StateChangeValueFinished;
  finishedReason?: DockerJobFinishedReason;
  // these restrict to workers with the same tags
  tags?: string[];
  namespaces?: string[];
  requirements?: {
    cpus?: number;
    gpus?: number;
    maxDuration?: string;
    memory?: string;
  };
}

/**
 * A job is its definition. A run is a job plus the inputs it processes, so one
 * job can process many inputs. See prepareEnqueueJob for how the ids are derived.
 */
export interface EnqueueJob {
  // The run id: what the queue, results and logs are keyed by. With no inputs
  // it is the same as jobHash.
  id: string;
  // The job's public id: jobHash, or a name the client chose (e.g. a uuid)
  jobId?: string;
  // sha256 of the definition: a version of the job
  jobHash?: string;
  // sha256 of the inputs
  inputsHash?: string;
  // Written to /inputs. Not part of the job's identity, only of the run's.
  inputs?: InputsRefs;
  definition: DockerJobDefinitionInputRefs;
  debug?: boolean;
  control?: DockerJobControlConfig;
}

export const enqueuedToInMemoryDockerJob = (enqueued: EnqueueJob): InMemoryDockerJob => {
  const queuedTime = Date.now();
  return {
    queuedTime: queuedTime,
    state: DockerJobState.Queued,
    time: queuedTime,
    worker: "",
    namespaces: enqueued.control?.namespace ? [enqueued.control.namespace] : [],
    tags: enqueued.definition.tags,
  };
};

export interface DockerJobDefinitionRow {
  // hash of the definition.
  hash: string;
  state: DockerJobState;
  value: DockerJobStateValue;
  history: StateChange[];
}

export const isDockerJobDefinitionRowFinished = (
  row: DockerJobDefinitionRow,
): boolean => {
  return row.state === DockerJobState.Finished;
};

export const getFinishedJobState = (
  row: DockerJobDefinitionRow,
): StateChangeValueFinished | undefined => {
  if (isDockerJobDefinitionRowFinished(row)) {
    return row.value as StateChangeValueFinished;
  }
};

export type JobsStateMap = Record<string, InMemoryDockerJob>;

export interface JobStates {
  jobs: JobsStateMap;
}

export interface WorkerRegistration {
  version: string;
  id: string;
  cpus: number;
  gpus: number;
  time: number;
  maxJobDuration: string;
}

export interface WorkerStatusResponse {
  time: number;
  id: string;
  cpus: number;
  gpus: number;
  queue: Record<string, { jobId: string; finished: boolean }>;
  maxJobDuration?: string;
}

export interface JobStatusPayload {
  jobId: string;
  step:
    | "docker image pull"
    | "cloning repo"
    | "docker build"
    | `${DockerJobState.Running}`
    | "docker image push";
  logs: ConsoleLogLine[];
}

export interface InstanceRegistration {
  instances: {
    id: string;
  }[];
}

/**
 * These are all the messsages types the worker sends to the api server
 */
export enum WebsocketMessageTypeWorkerToServer {
  StateChange = "StateChange",
  WorkerRegistration = "WorkerRegistration",
  WorkerStatusResponse = "WorkerStatusResponse",
  JobStatusLogs = "JobStatusLogs",
  RequestJobDefinitions = "RequestJobDefinitions",
}

export interface RequestJobDefinitions {
  jobIds: string[];
}

export interface WebsocketMessageWorkerToServer {
  type: WebsocketMessageTypeWorkerToServer;
  payload:
    | StateChange
    | WorkerRegistration
    | WorkerStatusResponse
    | JobStatusPayload
    | RequestJobDefinitions;
}
export type WebsocketMessageSenderWorker = (
  message: WebsocketMessageWorkerToServer,
) => void;

/**
 * These are all the messsages types the (browser) clients send to the api server
 */
export enum WebsocketMessageTypeClientToServer {
  StateChange = "StateChange",
  QueryJob = "QueryJob",
  QueryJobStates = "QueryJobStates",
}
export interface PayloadClearJobCache {
  jobId: string;
  namespace: string;
}

export interface PayloadResubmitJob {
  enqueued: EnqueueJob;
}

export interface PayloadClearJobOnWorker {
  jobId: string;
}

export interface PayloadQueryJob {
  jobId: string;
}

export interface WebsocketMessageClientToServer {
  type: WebsocketMessageTypeClientToServer;
  payload:
    | StateChange
    | PayloadResubmitJob
    | PayloadQueryJob;
}
export type WebsocketMessageSenderClient = (
  message: WebsocketMessageClientToServer,
) => void;

/**
 * These are all the messsages types the server sends to:
 *   - the (browser) clients
 *   - the workers
 */
export enum WebsocketMessageTypeServerBroadcast {
  // All jobs in the queue
  JobStates = "JobStates",
  // Updated jobs, not declarative of the entire queue
  JobStateUpdates = "JobStateUpdates",
  // logs from the worker
  JobStatusPayload = "JobStatusPayload",
  Workers = "Workers",
  StatusRequest = "StatusRequest",
  // Only the worker listens to this
  ClearJobCache = "ClearJobCache",
  BroadcastJobDefinitions = "BroadcastJobDefinitions",
}
export interface WebsocketMessageServerBroadcast {
  type: WebsocketMessageTypeServerBroadcast;
  payload:
    | BroadcastJobStates
    | BroadcastWorkers
    | BroadcastStatusRequest
    | PayloadClearJobCache
    | JobStatusPayload
    | BroadcastJobDefinitions;
}

export interface BroadcastJobDefinitions {
  definitions: Record<string, DockerJobDefinitionInputRefs>;
}
/**
 * The job states, not the jobs themselves
 */
export interface BroadcastJobStates {
  isSubset?: boolean;
  state: JobStates;
}

/**
 * Let everyone know how many workers and their resources
 */
export interface BroadcastWorkers {
  workers: WorkerRegistration[];
}

export type BroadcastStatusRequest = undefined;

/************************************************************
 * Client specific
 ************************************************************/

export type DockerJobDefinitionParamsInUrlHash = Omit<
  DockerJobDefinitionInputRefs,
  "inputs"
>;

// this is the actual job definition consumed by the workers
export interface DockerJobDefinitionMetadata {
  // the run id, see EnqueueJob.id
  hash: string;
  jobId?: string;
  jobHash?: string;
  inputsHash?: string;
  inputs?: InputsRefs;
  definition: DockerJobDefinitionInputRefs;
  debug?: boolean;
  maxJobDuration?: string;
  control?: DockerJobControlConfig;
}

/************************************************************
 * End Client specific
 ************************************************************/

/**
 * Only Finished jobs can have the cached state deleted
 * Queued or Running jobs should not be deleted
 * @param state
 * @returns
 */
export const isJobCacheAllowedToBeDeleted = (
  state: DockerJobState,
): boolean => {
  // This is duplicated (search for it, turn into function)
  switch (state) {
    case DockerJobState.Queued:
    case DockerJobState.Running:
      // not touching the job since it's active. Finish it first.
      return false;
    case DockerJobState.Finished:
      return true;
    default:
      return false;
  }
};

export type DockerApiDeviceRequest = {
  Driver: string;
  Count?: number;
  DeviceIDs?: string[];
  Capabilities: string[][];
};

export type DockerApiCopyJobToQueuePayload = {
  jobId: string;
  queue: string;
  control?: DockerJobControlConfig;
};
