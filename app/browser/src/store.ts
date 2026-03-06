import pDebounce from "p-debounce";
import { create } from "zustand";
import {
  BroadcastWorkers,
  ConsoleLogLine,
  DockerJobDefinitionMetadata,
  DockerJobFinishedReason,
  DockerJobState,
  getJobColorizedString,
  InMemoryDockerJob,
  isFinishedStateWorthCaching,
  isJobDeletedOrRemoved,
  JobsStateMap,
  JobStatusPayload,
  PayloadQueryJob,
  resolveMostCorrectJob,
  setJobStateFinished,
  StateChange,
  StateChangeValueFinished,
  StateChangeValueQueued,
  WebsocketMessageClientToServer,
  WebsocketMessageSenderClient,
  WebsocketMessageServerBroadcast,
  WebsocketMessageTypeClientToServer,
} from "/@shared/client";

import { getHashParamValueJsonFromWindow, setHashParamValueJsonInWindow } from "@metapages/hash-query";

import { cache } from "./cache";
import { LogsMode } from "./components/sections/logs/DisplayLogs";

let _cachedMostRecentSubmit: WebsocketMessageClientToServer | undefined;

export const cacheInsteadOfSendMessages = (message: WebsocketMessageClientToServer) => {
  if (
    message.type === WebsocketMessageTypeClientToServer.StateChange &&
    (message.payload as StateChange).state === DockerJobState.Queued
  ) {
    _cachedMostRecentSubmit = message;
  }
};

export type JobStateTuple = [string | undefined, InMemoryDockerJob | undefined];
const EmptyJobStateTuple: JobStateTuple = [undefined, undefined];

interface MainStore {
  /**
   * When the client creates a new job, it goes here.
   * This is NOT from the server
   */
  newJobDefinition: DockerJobDefinitionMetadata | undefined;
  setNewJobDefinition: (job: DockerJobDefinitionMetadata) => void;
  submitJob: () => void;
  queryJob: () => Promise<void>;

  /**
   * This is the state of our current job, sent from the server.
   */
  jobState: [string, InMemoryDockerJob] | null;
  setJobState: (job: [string, InMemoryDockerJob] | null) => void;

  /* We send the new job this way */
  sendClientStateChange: (payload: StateChange) => Promise<void>;

  /* The server sends job states, we get our current job state from this */
  jobStates: JobsStateMap;
  setJobStates: (jobStates: JobsStateMap, subset?: boolean) => void;
  cancelJob: () => void;
  deleteJobCache: () => Promise<boolean>;
  resubmitJob: () => void;

  /* To display all the workers */
  workers: BroadcastWorkers | undefined;
  setWorkers: (workers: BroadcastWorkers) => void;

  isServerConnected: boolean;
  setIsServerConnected: (isServerConnected: boolean) => void;

  /** Sends the websocket message to the API server */
  sendMessage: WebsocketMessageSenderClient;
  setSendMessage: (sendMessage: WebsocketMessageSenderClient) => void;

  /** Sends the websocket message to the API server */
  rawMessage: WebsocketMessageServerBroadcast | undefined;
  setRawMessage: (rawMessage: WebsocketMessageServerBroadcast) => void;

  /**
   * Logs streamed from the build step.
   * They are not cached anywhere.
   */
  buildLogs: ConsoleLogLine[] | null;
  setBuildLogs: (logs: ConsoleLogLine[] | null) => void;
  appendBuildLogs: (logs: ConsoleLogLine[] | null) => void;

  /**
   * Logs from the run step. They are streamed live from
   * the server: the streamed logs are not cached.
   * When the job is done, you get the logs (if any)
   */
  runLogs: ConsoleLogLine[] | null;
  setRunLogs: (logs: ConsoleLogLine[] | null) => void;
  appendRunLogs: (logs: ConsoleLogLine[] | null) => void;

  handleJobStatusPayload: (status: JobStatusPayload) => void;

  setRightPanelContext: (context: string | null) => void;
  rightPanelContext: string | null;

  mainInputFile: string | null;
  setMainInputFile: (context: string | null) => void;
  mainInputFileContent: string | null;
  setMainInputFileContent: (mainInputFileContent: string | null) => void;

  saveInputFileAndRun: () => void;

  userClickedRun: boolean;
  setUserClickedRun: (userClickedRun: boolean) => void;

  copyLogsToClipboard: (mode: LogsMode) => void;
}

/**
 * This is where two complex hooks are threaded together:
 * 1. get the job definition
 * 2. send the job definition if changed
 * 3. Show the status of the current job, and allow cancelling
 * 4. If the current job is finished, send the outputs (once)
 */
export const useStore = create<MainStore>((set, get) => ({
  // This is only used to figure out if the job outputs should
  // be sent to the metaframe outputs when the metaframe starts
  // The hash param jobStartsAutomatically is also checked.
  userClickedRun: false,
  setUserClickedRun: (userClickedRun: boolean) => {
    set(() => ({ userClickedRun }));
  },

  // Stores the latest job definition + inputs
  newJobDefinition: undefined,
  setNewJobDefinition: async (job: DockerJobDefinitionMetadata) => {
    // Update the local job hash (id) on change
    if (!job) {
      set(() => ({
        newJobDefinition: undefined,
        jobState: EmptyJobStateTuple,
        jobId: undefined,
        buildLogs: null,
        runLogs: null,
      }));
      return;
    }

    if (get().newJobDefinition?.hash === job.hash) {
      // no change.
      // But we update the state anyway, in case the job state changed
      let currentState = get().jobStates[job.hash];
      const cachedFinishedState = await cache.getFinishedJob(job.hash);
      if (cachedFinishedState && !isJobDeletedOrRemoved(cachedFinishedState)) {
        currentState = cachedFinishedState;
      }

      set(() => ({
        jobState: get().jobStates[job.hash] ? [job.hash, currentState] : EmptyJobStateTuple,
      }));
      return;
    }

    set(() => ({
      newJobDefinition: job,
      jobState: get().jobStates[job.hash] ? [job.hash, get().jobStates[job.hash]] : [job.hash, undefined],
      buildLogs: null,
      runLogs: null,
    }));
  },

  submitJob: pDebounce(async () => {
    // console.log(`store.submitJob`);
    const definitionBlob = get().newJobDefinition;
    if (!definitionBlob) {
      // console.log("submitJob: no definitionBlob");
      return;
    }

    // check the cache for a finished job, likely to remove
    const cachedFinishedState = await cache.getFinishedJob(definitionBlob.hash);
    if (cachedFinishedState) {
      await cache.deleteFinishedJob(definitionBlob.hash);
    }

    // inputs are already minified (fat blobs uploaded to the cloud)
    const value: StateChangeValueQueued = {
      type: DockerJobState.Queued,
      time: Date.now(),
      enqueued: {
        id: definitionBlob.hash,
        definition: definitionBlob.definition,
        control: definitionBlob.control,
      },
    };

    if (definitionBlob.debug) {
      value.enqueued.debug = true;
    }
    const payload: StateChange = {
      state: DockerJobState.Queued,
      value,
      job: definitionBlob.hash,
      tag: "", // document the meaning of this. It's the worker claim. Might be unneccesary due to history
    };

    // console.log(`store.submitJob: sending client state change`);
    get().sendClientStateChange(payload);
  }, 200),

  queryJob: pDebounce(async () => {
    const definitionBlob = get().newJobDefinition;
    if (!definitionBlob) {
      return;
    }

    const payload: PayloadQueryJob = {
      jobId: definitionBlob.hash,
    };
    // otherwise, just send the state change
    get().sendMessage({
      type: WebsocketMessageTypeClientToServer.QueryJob,
      payload,
    });

    // check if the job is already finished
    const finishedState = await cache.getFinishedJob(definitionBlob.hash);
    if (finishedState && !isJobDeletedOrRemoved(finishedState)) {
      const jobStates = { ...get().jobStates };
      jobStates[definitionBlob.hash] = finishedState;
      get().setJobStates(jobStates);
    }
  }, 200),

  jobStates: {},
  setJobStates: async (incomingJobStates: JobsStateMap, subset = false) => {
    // the finished state can be large, so it's stored in s3
    const newJobStates = subset ? { ...get().jobStates, ...incomingJobStates } : incomingJobStates;
    const jobHash = get().newJobDefinition?.hash;

    // console.log(`setJobStates.1 ${getJobColorizedString(jobHash)} ${getJobStateString(newJobStates[jobHash])} 👀 store.setJobStates`);

    if (jobHash) {
      const serverJobState = newJobStates[jobHash];
      const cachedFinishedState = await cache.getFinishedJob(jobHash);
      if (cachedFinishedState && serverJobState) {
        const mostCorrectJob = resolveMostCorrectJob(serverJobState, cachedFinishedState);
        if (mostCorrectJob !== serverJobState) {
          // console.log(`setJobStates.2 ${getJobColorizedString(jobHash)} ${getJobStateString(mostCorrectJob)} 👀 store.setJobStates resolveMostCorrectJob is different`);
          newJobStates[jobHash] = mostCorrectJob;
        }
      } else if (cachedFinishedState && !serverJobState) {
        // use the local cache!
        newJobStates[jobHash] = {
          ...cachedFinishedState,
        };
      }
    }

    const replacementsFromCache: JobsStateMap = {};
    for (const [jobId, job] of Object.entries(newJobStates)) {
      // download and cache the potentially large finished job payloads
      const replacement = await cache.processJob(jobId, job);
      if (replacement !== job) {
        replacementsFromCache[jobId] = replacement;
      }
    }
    // console.log(`setJobStates.3 ${getJobColorizedString(jobHash)} ${getJobStateString(replacementsFromCache[jobHash])} 👀 store.setJobStates after cache.processJob`);

    for (const [jobId, job] of Object.entries(replacementsFromCache)) {
      newJobStates[jobId] = job;
    }

    if (newJobStates[jobHash]) {
      // check for data that has changed shape and set the job to error so it can be deleted
      if ((newJobStates[jobHash] as { reason?: string })["reason"]) {
        console.log("Found incompatible job, converted to error to allow deletion", newJobStates[jobHash]);
        // set to errored
        newJobStates[jobHash] = {
          ...newJobStates[jobHash],
          state: DockerJobState.Finished,
          finishedReason: DockerJobFinishedReason.Error,
          time: Date.now(),
        };
      }
    }

    const serverJobState = newJobStates[jobHash];
    set(() => ({ jobStates: newJobStates }));
    // Set the job state(s) from the server
    get().setJobState([jobHash, serverJobState]);
  },

  jobState: EmptyJobStateTuple,
  setJobState: async (args: JobStateTuple) => {
    if (!args) {
      set(() => ({
        jobState: EmptyJobStateTuple,
        buildLogs: null,
        runLogs: null,
      }));
      return;
    }
    const [jobId, jobState] = args;

    // console.log(`${jobId}=${getJobStateString(jobState)} 👀 store.setJobState: `);

    set(() => ({ jobState: [jobId, jobState] }));
    if (!jobState || isJobDeletedOrRemoved(jobState)) {
      set(() => ({
        buildLogs: null,
        runLogs: null,
      }));
    } else if (jobState && jobState?.state === DockerJobState.Queued) {
      set(() => ({
        buildLogs: null,
        runLogs: null,
      }));
    } else if (jobState?.state === DockerJobState.Finished) {
      // if the job is finished, logs come from the result
      // not the cached streaming logs
      set(() => ({
        runLogs: jobState?.finished?.result?.logs,
      }));
    }
  },

  // This tells the server connection to send, but we check for
  // cached jobs first
  sendClientStateChange: async (clientStateChange: StateChange) => {
    // check if it's queued and an existing finished job exists.
    // If so, set the job state to finished, with the cached finished state
    // This means the state change doesn't reach the server+worker
    if (clientStateChange.state === DockerJobState.Queued) {
      const existingFinishedJob = await cache.getFinishedJob(clientStateChange.job);
      if (existingFinishedJob?.finishedReason && isFinishedStateWorthCaching(existingFinishedJob?.finishedReason)) {
        // console.log(
        //   `${getJobColorizedString(clientStateChange.job)} ✅ 🐼 Found existing finished job for ${clientStateChange.job}`,
        //   existingFinishedJob,
        // );
        const currentJobStates = get().jobStates;
        const newJobStates = {
          ...currentJobStates,
          [clientStateChange.job]: existingFinishedJob,
        };
        get().setJobStates(newJobStates);
        return;
      }
    }
    // otherwise, just send the state change
    // console.log(`store.sendClientStateChange: ACTUALLYsending client state change`, clientStateChange);
    get().sendMessage({
      type: WebsocketMessageTypeClientToServer.StateChange,
      payload: clientStateChange,
    });
  },

  cancelJob: async () => {
    // console.log(`🚩🚩🚩 store.cancelJob`);
    const [jobId, job] = get().jobState;
    if (!jobId || !job) {
      return;
    }

    // the server state change is cancelled, but our local state is deleted
    // so that if a finished state arrives, it will not overwrite the deleted state
    const cancelledChange: StateChangeValueFinished = {
      type: DockerJobState.Finished,
      reason: DockerJobFinishedReason.Cancelled,
      time: Date.now(),
      namespace: get().newJobDefinition?.control?.namespace,
    };

    const canceledJob: InMemoryDockerJob = setJobStateFinished(job, { finished: cancelledChange });
    await cache.saveFinishedJob(jobId, canceledJob);
    // await cache.deleteFinishedJob(jobId);
    const newJobStates = {
      ...get().jobStates,
    };
    newJobStates[jobId] = canceledJob;
    get().setJobStates(newJobStates);

    const stateChange: StateChange = {
      tag: "",
      state: DockerJobState.Finished,
      job: jobId,
      value: {
        type: DockerJobState.Finished,
        reason: DockerJobFinishedReason.Cancelled,
        message: "Job cancelled by client",
        time: Date.now(),
      },
    };
    get().sendClientStateChange(stateChange);
  },

  resubmitJob: async () => {
    const jobState = get().jobState;
    if (!jobState) {
      return;
    }
    await get().deleteJobCache();

    if (!get().newJobDefinition?.definition) {
      return;
    }

    await new Promise(resolve => setTimeout(resolve, 1000));

    get().submitJob();
  },

  deleteJobCache: async () => {
    const client = get().newJobDefinition;
    if (!client?.hash) {
      return false;
    }

    const deletedChange: StateChangeValueFinished = {
      type: DockerJobState.Finished,
      reason: DockerJobFinishedReason.Deleted,
      time: Date.now(),
      namespace: get().newJobDefinition?.control?.namespace,
    };

    const currentJobState = get().jobStates[client.hash] || {
      state: DockerJobState.Finished,
      finishedReason: DockerJobFinishedReason.Deleted,
      finished: deletedChange,
      time: Date.now(),
      queuedTime: Date.now(),
    };

    let deletedJob: InMemoryDockerJob = setJobStateFinished(currentJobState, { finished: deletedChange });
    deletedJob = setJobStateFinished(deletedJob, { finished: deletedChange });
    await cache.saveFinishedJob(client.hash, deletedJob);

    // send a delete message to the server
    const stateChange: StateChange = {
      tag: "",
      state: DockerJobState.Finished,
      job: client.hash,
      value: deletedChange,
    };
    get().sendClientStateChange(stateChange);

    const newJobStates = {
      ...get().jobStates,
    };
    newJobStates[client.hash] = deletedJob;

    get().setJobStates(newJobStates);

    return true;
  },

  workers: undefined,
  setWorkers: (workers: BroadcastWorkers) => {
    set(() => ({ workers }));
  },

  isServerConnected: false,
  setIsServerConnected: (isServerConnected: boolean) => {
    set(() => ({ isServerConnected }));
  },

  // the initial sendMessage just caches the messages to send later
  sendMessage: cacheInsteadOfSendMessages,

  setSendMessage: (sendMessage: WebsocketMessageSenderClient) => {
    // Send the cached messages
    if (sendMessage !== cacheInsteadOfSendMessages) {
      const msg = _cachedMostRecentSubmit;
      _cachedMostRecentSubmit = undefined;
      sendMessage(msg);
    }
    set(() => ({ sendMessage }));
  },

  rawMessage: undefined,
  setRawMessage: (rawMessage: WebsocketMessageServerBroadcast) => {
    set(() => ({ rawMessage }));
  },

  buildLogs: null,
  setBuildLogs: (logs: ConsoleLogLine[] | null) => {
    set(() => ({ buildLogs: logs }));
  },
  appendBuildLogs: (logs: ConsoleLogLine[] | null) => {
    if (!logs || logs.length === 0) {
      return;
    }
    set(() => ({ buildLogs: [...(get().buildLogs || []), ...logs] }));
  },

  runLogs: null,
  setRunLogs: (logs: ConsoleLogLine[] | null) => {
    set(() => ({ runLogs: logs }));
  },
  appendRunLogs: (logs: ConsoleLogLine[] | null) => {
    if (!logs || logs.length === 0) {
      return;
    }
    set(() => ({ runLogs: [...(get().runLogs || []), ...logs] }));
  },

  handleJobStatusPayload: (status: JobStatusPayload) => {
    if (
      !get().jobState?.[0] ||
      get().jobState?.[0] !== status?.jobId ||
      get().jobState?.[1]?.state !== DockerJobState.Running
    ) {
      return;
    }
    switch (status.step) {
      case "docker image push":
        // TODO: do something with the push logs?
        break;
      case "docker image pull":
      case "cloning repo":
      case "docker build":
        get().appendBuildLogs(status.logs);
        break;
      case `${DockerJobState.Running}`:
        get().appendRunLogs(status.logs);
        break;
      default:
        console.error(`${getJobColorizedString(status.jobId)} ❌ Unknown job step:`, status.step);
        break;
    }
  },

  setRightPanelContext: (rightPanelContext: string | null) => {
    set(() => ({ rightPanelContext }));
  },
  // rightPanelContext: "editScript",
  rightPanelContext: null,

  mainInputFile: null,
  setMainInputFile: (mainInputFile: string | null) => {
    set(() => ({ mainInputFile }));
  },

  mainInputFileContent: null,
  setMainInputFileContent: (mainInputFileContent: string | null) => {
    set(() => ({ mainInputFileContent }));
  },

  saveInputFileAndRun: () => {
    if (!get().mainInputFile || !get().mainInputFileContent) {
      return;
    }
    const currentJobId = get().newJobDefinition?.hash;
    const unsubscribe = useStore.subscribe(state => {
      if (state.newJobDefinition?.hash !== currentJobId) {
        unsubscribe();
        get().submitJob();
      }
    });
    const inputs: Record<string, string> = getHashParamValueJsonFromWindow("inputs") || {};
    inputs[get().mainInputFile] = get().mainInputFileContent;
    setHashParamValueJsonInWindow("inputs", inputs);
    get().setMainInputFileContent(null);
  },

  copyLogsToClipboard: (mode: LogsMode) => {
    let logs = [];
    switch (mode) {
      case "stdout+stderr":
        logs = get().runLogs || [];
        break;
      case "stdout":
        logs = (get().runLogs || []).filter(log => !log[2]);
        break;
      case "stderr":
        logs = (get().runLogs || []).filter(log => log[2]);
        break;
      case "build":
        logs = get().buildLogs || [];
        break;
    }
    if (!logs || logs.length === 0) {
      return;
    }
    const allLogsText = logs.map(log => log[0]).join("\n");
    navigator?.clipboard?.writeText(allLogsText);
  },
}));
