import { ms } from "ms";
import parseDuration from "parse-duration";
import { config } from "/@/config.ts";
import { ensureIsolateNetwork } from "/@/docker/network.ts";
import { convertIOToVolumeMounts, getOutputs } from "/@/queue/IO.ts";
import { convertStringToDockerCommand, getDockerFiltersForJob } from "/@/queue/utils.ts";

import * as computeQueuesShared from "@metapages/compute-queues-shared";
import {
  type DockerJobDefinitionInputRefs,
  DockerJobState,
  getJobColorizedString,
  getWorkerColorizedString,
  type InMemoryDockerJob,
  resolvePreferredWorker,
} from "@metapages/compute-queues-shared";

import mod from "../../mod.json" with { type: "json" };
import { ContainerLabelId, ContainerLabelWorker } from "./constants.ts";
import { docker } from "./dockerClient.ts";
import { dockerJobExecute } from "./DockerJob.ts";
import { getRunningContainerForJob } from "./index.ts";
import type { JobDefinitionCache } from "./JobDefinitionCache.ts";
import {
  type DockerJobArgs,
  type DockerJobExecution,
  type DockerJobQueueArgs,
  DockerRunPhase,
  type Volume,
  type WorkerJobQueueItem,
} from "./types.ts";

const Version: string = mod.version;

export class DockerJobQueue {
  maxJobDuration: number;
  maxJobDurationString: string;
  workerId: string;
  workerIdShort: string;
  cpus: number;
  gpus: number;
  // space in the value structure for local state
  // These are RUNNING jobs
  queue: { [hash in string]: WorkerJobQueueItem } = {};
  apiQueue: Record<string, InMemoryDockerJob> = {};

  // If we finish a job but the server is unavailabe when we request a stateChange
  // then we persist (for some interval (1 week?)) the stateChange so that when the
  // server reconnects, we can send the results
  // cachedResults: any = {};
  // Tell the server our state change requests
  sender: computeQueuesShared.WebsocketMessageSenderWorker;

  queueKey: string;
  jobDefinitions: JobDefinitionCache;
  private registrationInterval: number | null = null;
  private runningJobHealthInterval: number | null = null;
  gotFirstCompleteJobState: boolean = false;
  performedInitialContainerCheckAfterJobState: boolean = false;

  constructor(args: DockerJobQueueArgs) {
    const { sender, cpus, gpus, id, maxJobDuration, queue, jobDefinitions } = args;
    this.cpus = cpus;
    this.gpus = gpus;
    this.sender = sender;
    this.workerId = id;
    this.workerIdShort = getWorkerColorizedString(this.workerId);
    this.maxJobDurationString = maxJobDuration;
    this.maxJobDuration = parseDuration(maxJobDuration) as number;
    this.queueKey = queue;
    this.jobDefinitions = jobDefinitions;
    if (!this.queueKey) {
      throw new Error("queueKey is required");
    }
    // this.checkForLocallyRunningJobs();

    // Start periodic registration
    this.startPeriodicRegistration();

    // Start periodic running job health check
    this.startRunningJobHealthInterval();

    // this.killOrphanedJobs();
  }

  async checkForLocallyRunningJobs() {
    const runningContainers = await docker.listContainers({
      filters: getDockerFiltersForJob({ workerId: this.workerId, status: "running" }),
    });

    for (const containerData of runningContainers) {
      const containerId = containerData.Id;
      try {
        const container = docker.getContainer(containerId);
        if (!container) {
          continue;
        }

        const containerInfo = await container.inspect();

        const jobId = containerInfo.Config?.Labels?.[ContainerLabelId];
        if (jobId && !this.queue[jobId]) {
          console.log(
            `${this.workerIdShort} 👀 found existing running container for job but not in our internalqueue: ${
              getJobColorizedString(jobId)
            }`,
          );
          container?.kill();
        }
      } catch (err) {
        /* do nothing */
        console.error(err);
      }
    }
  }

  gpuDeviceIndicesUsed(): number[] {
    const gpuDeviceIndicesUsed: number[] = Object.values(this.queue)
      .filter((item: WorkerJobQueueItem) => item.gpuIndices)
      .reduce<number[]>((array, item) => {
        return item.gpuIndices ? array.concat(item.gpuIndices) : array;
      }, []);
    gpuDeviceIndicesUsed.sort();
    return gpuDeviceIndicesUsed;
  }

  gpuCapacity(): number {
    return this.gpus - this.gpuDeviceIndicesUsed().length;
  }

  isGPUCapacity(): boolean {
    return this.gpuCapacity() > 0;
  }

  /**
   * Get available GPU device indices for this worker
   */
  getAvailableGpuDevices(): number[] {
    const usedIndices = this.gpuDeviceIndicesUsed();
    const { deviceIndices } = config.gpuConfig;

    if (deviceIndices.length > 0) {
      // Use specific device indices specified in config
      return deviceIndices.filter((index) => !usedIndices.includes(index));
    } else {
      // Legacy mode: use sequential allocation (0, 1, 2, ...)
      const availableDevices: number[] = [];
      for (let i = 0; i < this.gpus; i++) {
        if (!usedIndices.includes(i)) {
          availableDevices.push(i);
        }
      }
      return availableDevices;
    }
  }

  // getGPUDeviceRequests() :{
  //     Driver:string,
  //     Count: number,
  //     DeviceIDs?: string[],
  //     Capabilities: string[][]
  //   }[] {
  //     if (!this.isGPUCapacity()) {
  //         throw `getGPUDeviceRequests but no capacity`;
  //     }
  //     const gpuDeviceIndicesUsed :number[] = Object.values(this.queue)
  //         .filter((item :WorkerJobQueueItem) => item.gpuIndices)
  //         .reduce<number[]>((array, item) => {
  //             return item.gpuIndices ? array.concat(item.gpuIndices) : array;
  //         }, []);

  //     gpuDeviceIndicesUsed.sort();
  //     // Now get thei first available GPU

  //     for (let gpuIndex = 0; gpuIndex < this.gpus; gpuIndex++) {
  //         if (!gpuDeviceIndicesUsed.includes(gpuIndex)) {
  //             return [{
  //                 Driver: 'nvidia',
  //                 Count: 1,
  //                 DeviceIDs: [`${gpuIndex}`],
  //                 Capabilities: [["gpu"]],
  //             }];
  //         }
  //     }

  //     throw `getGPUDeviceRequests but could not find an available GPU`;
  // }

  getGPUDeviceIndex(): number {
    if (!this.isGPUCapacity()) {
      throw `getGPUDeviceIndex but no capacity`;
    }

    const availableDevices = this.getAvailableGpuDevices();
    if (availableDevices.length === 0) {
      throw `getGPUDeviceIndex but could not find an available GPU`;
    }

    // Return the first available GPU device index
    return availableDevices[0];
  }

  status(): computeQueuesShared.WorkerStatusResponse {
    const runningJobsCount = Object.keys(this.queue).length;
    const availableGpuDevices = this.getAvailableGpuDevices();
    const usedGpuDevices = this.gpuDeviceIndicesUsed();
    console.log(
      `${this.workerIdShort} status: ${runningJobsCount} running jobs, ${this.cpus} CPUs, ${this.gpus} GPUs (available: [${
        availableGpuDevices.join(",")
      }], used: [${usedGpuDevices.join(",")}])`,
    );

    return {
      time: Date.now(),
      id: this.workerId,
      cpus: this.cpus,
      gpus: this.gpus,
      queue: Object.fromEntries(
        Object.entries(this.queue).map(([key, item]) => {
          return [
            key,
            {
              jobId: key,
              definition: item.definition,
              finished: !!item.execution,
            },
          ];
        }),
      ),
    };
  }

  register() {
    const registration: computeQueuesShared.WorkerRegistration = {
      time: Date.now(),
      version: Version,
      id: this.workerId,
      cpus: this.cpus,
      gpus: this.gpus,
      maxJobDuration: this.maxJobDurationString,
    };

    try {
      this.sender({
        type: computeQueuesShared.WebsocketMessageTypeWorkerToServer
          .WorkerRegistration,
        payload: registration,
      });

      // Send running jobs state (the message is created once
      // and tells the server the running jobs)
      for (const runningQueueObject of Object.values(this.queue)) {
        this.sender(runningQueueObject.runningMessageToServer);
      }

      console.log(`${this.workerIdShort} 🌏 registered successfully`);
    } catch (err) {
      console.log(
        `${this.workerIdShort} 🚨 Failed to register worker : ${err}`,
      );

      // Retry registration after a delay
      setTimeout(() => {
        try {
          this.sender({
            type: computeQueuesShared.WebsocketMessageTypeWorkerToServer
              .WorkerRegistration,
            payload: registration,
          });
          console.log(
            `${this.workerIdShort} registration retry successful`,
          );
        } catch (retryErr) {
          console.log(
            `${this.workerIdShort} registration retry failed: ${retryErr}`,
          );
        }
      }, 2000);
    }
  }

  private startPeriodicRegistration() {
    // Register every 30 seconds to ensure server always knows about this worker
    // This is especially important after clean server restarts
    this.registrationInterval = setInterval(() => {
      try {
        this.register();
      } catch (err) {
        console.log(
          `${this.workerIdShort} 🚨 Periodic registration failed: ${err}`,
        );
      }
    }, ms("10s") as number);
  }

  public stopPeriodicRegistration() {
    if (this.registrationInterval) {
      clearInterval(this.registrationInterval);
      this.registrationInterval = null;
    }
  }

  async onUpdateSetAllJobStates(
    message: computeQueuesShared.BroadcastJobStates,
  ) {
    message.isSubset = false;
    if (Object.keys(message?.state?.jobs || {}).length > 0 || !message.isSubset) {
      console.log(
        `${getWorkerColorizedString(this.workerId)} JobStateUpdates [isSubset=${message.isSubset}] from server: ${
          Object.keys(message?.state?.jobs || {}).map((jobId) =>
            getJobColorizedString(jobId) +
            `(server says: ${message.state.jobs[jobId].state}, local says: phase=${
              this.queue[jobId]?.phase
            }, executionExists=${!!this.queue[jobId]?.execution}, executionKilled=${
              this.queue[jobId]?.execution?.isKilled.value ? "KILLED" : "ALIVE"
            })`
          ).join(", ")
        }`,
      );
    }
    this._updateApiQueue(message);
    this._checkRunningJobs();
    await this._claimJobs();

    if (!message.isSubset) {
      this.gotFirstCompleteJobState = true;
    }

    if (this.gotFirstCompleteJobState && !this.performedInitialContainerCheckAfterJobState) {
      this.performedInitialContainerCheckAfterJobState = true;
      await this.killOrphanedJobs();
      await this.checkForLocallyRunningJobs();
    }
  }

  async onUpdateUpdateASubsetOfJobs(
    message: computeQueuesShared.BroadcastJobStates,
  ) {
    message.isSubset = true;
    this._updateApiQueue(message);
    console.log(
      `${getWorkerColorizedString(this.workerId)} after _updateApiQueue apiQueue: ${
        Object.keys(this.apiQueue || {}).map((jobId) =>
          getJobColorizedString(jobId) + `(${this.apiQueue[jobId].state})`
        ).join(", ")
      }`,
    );
    this._checkRunningJobs();
    console.log(
      `${getWorkerColorizedString(this.workerId)} before _claimJobs this.queue: ${
        Object.keys(this.queue || {}).map((jobId) =>
          getJobColorizedString(jobId) + `(${this.queue[jobId]?.execution?.isKilled.value ? "KILLED" : "ALIVE"})`
        ).join(", ")
      }`,
    );
    await this._claimJobs();
  }

  _updateApiQueue(message: computeQueuesShared.BroadcastJobStates) {
    const jobStates = message.state.jobs;

    if (!message.isSubset) {
      // set the value from the server, this is the entire queue
      this.apiQueue = jobStates;
    } else {
      Object.keys(jobStates).forEach((jobId) => {
        this.apiQueue[jobId] = jobStates[jobId];
      });
    }
    // Remove finished jobs from the apiQueue
    // Why? We might need to send the finished jobs to the server even
    // if another worker has taken and finished it, because they might
    // have errored out.
    // Object.entries(this.apiQueue).forEach(([jobId, job]) => {
    //   if (job.state === computeQueuesShared.DockerJobState.Finished) {
    //     delete this.apiQueue[jobId];
    //   }
    // });

    // Prefetch the job definitions for the jobs in the queue
    // this will also only prefetch the jobs that are not already in the cache
    this.jobDefinitions.prefetch(Object.keys(jobStates));
  }

  _checkRunningJobs() {
    // console.log(`${this.workerIdShort} _checkRunningJobs:`, message);
    const jobStates = this.apiQueue;
    // const isAllJobs = !message.isSubset;

    if (config.debug) {
      console.log(
        `${this.workerIdShort} 🎳 _checkRunningJobs this.apiQueue=[${Object.keys(this.apiQueue).join(",")}]`,
      );
    }

    // make sure our local jobs should be running (according to the server state)
    for (const [locallyRunningJobId, locallyRunningJob] of Object.entries(this.queue)) {
      // do we have local jobs the server doesn't even know about? This should never happen
      // Maybe it could be in the case where the browser disconnected and you want the jobs to keep going
      if (!this.apiQueue[locallyRunningJobId] || this.apiQueue[locallyRunningJobId].state === DockerJobState.Finished) {
        // we can only kill the job if we know it's not running on the server
        // if (isAllJobs) {
        console.log(
          `${this.workerIdShort} ${
            getJobColorizedString(locallyRunningJobId)
          } cannot find local job (or job Finished) in server state, setInterval to check then maybe kill and remove`,
        );

        // Do not remove immediately, because there might be a race condition
        setTimeout(() => {
          if (
            this.queue[locallyRunningJobId] &&
            (!this.apiQueue[locallyRunningJobId] ||
              this.apiQueue[locallyRunningJobId].state === DockerJobState.Finished)
          ) {
            console.log(
              `${this.workerIdShort} ${
                getJobColorizedString(locallyRunningJobId)
              } after some time, job is either missing from API or Finished, so killing our version`,
            );
            this._killJobAndIgnore(locallyRunningJobId);
          }
        }, ms("6s") as number);

        continue;
      }

      const serverJobState = jobStates[locallyRunningJobId];

      if (config.debug) {
        console.log(
          `${this.workerIdShort} 🎳 _checkRunningJobs new ${getJobColorizedString(locallyRunningJobId)}`,
          serverJobState,
        );
      }

      switch (serverJobState.state) {
        // are any jobs running locally actually killed by the server? or running
        case DockerJobState.Finished:
          // handled above
          // FINE it finished elsewhere, how rude
          // console.log(
          //   `${this.workerIdShort} ${
          //     getJobColorizedString(locallyRunningJobId)
          //   } finished elsewhere, killing here (likely JobReplacedByClient or cancelled)`,
          // );

          // if (serverJobState.worker !== this.workerId) {

          //   switch (serverJobState.finishedReason) {
          //     case DockerJobFinishedReason.WorkerLost:
          //     case DockerJobFinishedReason.JobReplacedByClient:
          //     case DockerJobFinishedReason.TimedOut:
          //       // the worker
          //       break;

          //     case DockerJobFinishedReason.JobFinished:
          //       break;
          //     case DockerJobFinishedReason.JobKilled:
          //   }
          //   console.log(
          //     `${this.workerIdShort} ${
          //       getJobColorizedString(locallyRunningJobId)
          //     } server state=Finished (possibly JobReplacedByClient) but server says running, killing and removing`,
          //   );
          // }
          // this._killJobAndIgnore(locallyRunningJobId, "server state=Finished (possibly JobReplacedByClient)");
          break;
        case DockerJobState.Queued:
          // server says queued, I say running, remind the server
          // this can easily happen if I submit Running, but then
          // the worker gets another update immediately
          // The server will ignore this if it gets multiple times
          // console.log(
          //   `${this.workerIdShort} ${
          //     getJobColorizedString(locallyRunningJobId)
          //   } server says queued, I say running, sending running again`,
          // );
          this.sender({
            type: computeQueuesShared.WebsocketMessageTypeWorkerToServer
              .StateChange,
            payload: {
              tag: this.workerId,
              job: locallyRunningJobId,
              state: computeQueuesShared.DockerJobState.Running,
              value: {
                worker: this.workerId,
                time: locallyRunningJob.time || Date.now(),
              } as computeQueuesShared.StateChangeValueRunning,
            },
          });
          break;
        case DockerJobState.Running:
          // good!
          // except if another worker has taken it, then kill ours (server is dictator)
          if (
            serverJobState.worker !== this.workerId
          ) {
            const preferredWorker = resolvePreferredWorker(
              this.workerId,
              serverJobState.worker,
            );
            if (preferredWorker === this.workerId) {
              console.log(
                `${this.workerIdShort} ${
                  getJobColorizedString(locallyRunningJobId)
                } running, but elsewhere apparently ${
                  getWorkerColorizedString(serverJobState.worker)
                }. We are keeping ours since we are preferred`,
              );
            } else {
              console.log(
                `${this.workerIdShort} ${
                  getJobColorizedString(locallyRunningJobId)
                } running, but elsewhere also, and preferred there: ${
                  getWorkerColorizedString(serverJobState.worker)
                }. setInterval to check then maybe kill`,
              );
              // Do not remove immediately, because there might be a race condition
              setTimeout(() => {
                if (!this.queue[locallyRunningJobId] || !this.apiQueue[locallyRunningJobId]) {
                  // gone!
                  return;
                }
                const isRunningElsewhereStill = this.apiQueue[locallyRunningJobId].state === DockerJobState.Running &&
                  this.apiQueue[locallyRunningJobId].worker !== this.workerIdShort;
                const preferredWorkerAfterSomeTime = resolvePreferredWorker(
                  this.workerId,
                  this.apiQueue[locallyRunningJobId].worker,
                );
                if (isRunningElsewhereStill && preferredWorkerAfterSomeTime !== this.workerId) {
                  console.log(
                    `${this.workerIdShort} ${
                      getJobColorizedString(locallyRunningJobId)
                    } after some time, job is still preferred by ${
                      getWorkerColorizedString(this.apiQueue[locallyRunningJobId].worker)
                    } , so killing our version`,
                  );
                  this._killJobAndIgnore(locallyRunningJobId);
                }
              }, ms("6s") as number);
            }
          }
          break;
      }
    }
  }

  private _isClaimingJobs: boolean = false;
  private _needsAnotherClaimJobs: boolean = false;
  async _claimJobs() {
    // If already running, set flag for another run and return

    if (config.debug) {
      console.log(
        `${this.workerIdShort} 🎳 _claimJobs this.queue=[${Object.keys(this.queue).join(",")}] this.apiQueue=[${
          Object.keys(this.apiQueue).join(",")
        }]`,
      );
    }
    if (this._isClaimingJobs) {
      if (config.debug) {
        console.log(
          `${this.workerIdShort} _claimJobs already running, setting flag for another run`,
        );
      }
      this._needsAnotherClaimJobs = true;
      return;
    }

    // Set running flag
    this._isClaimingJobs = true;
    try {
      do {
        this._needsAnotherClaimJobs = false;
        // const jobStates = message.state.jobs;

        // Original _claimJobs logic here
        const jobsServerSaysAreRunningOnMe = Object.keys(this.apiQueue).filter((
          key,
        ) =>
          this.apiQueue[key].state === computeQueuesShared.DockerJobState.Running &&
          this.apiQueue[key].worker === this.workerId
        );
        if (config.debug) {
          console.log(
            `${this.workerIdShort} 🎳 _claimJobs jobsServerSaysAreRunningOnMe=[${
              Object.keys(jobsServerSaysAreRunningOnMe).join(",")
            }]`,
          );
        }

        // check docker if they are?
        for (const runningJobId of jobsServerSaysAreRunningOnMe) {
          if (!this.queue[runningJobId]) {
            console.log(
              `${this.workerIdShort} 🎳 _claimJobs runningJobId=${
                getJobColorizedString(runningJobId)
              } jobsServerSaysAreRunningOnMe, starting`,
            );
            try {
              const definition = await this.jobDefinitions.get(runningJobId);
              if (definition) {
                this._startJob(runningJobId, definition);
              } else {
                console.log(
                  `${this.workerIdShort} ${getJobColorizedString(runningJobId)} 🎳 _claimJobs failed to get definition`,
                );
              }
            } catch (err) {
              console.log(
                `${this.workerIdShort} ${getJobColorizedString(runningJobId)} 🎳 _claimJobs failed to get definition`,
                err,
              );
            }
          }
        }

        // only care about queued jobs
        const queuedJobKeys: string[] = Object.keys(this.apiQueue)
          .filter((key) =>
            this.apiQueue[key].state ===
              computeQueuesShared.DockerJobState.Queued
          );

        queuedJobKeys.sort((a, b) => {
          const aQueuedTime = this.apiQueue[a].queuedTime;
          const bQueuedTime = this.apiQueue[b].queuedTime;
          return aQueuedTime - bQueuedTime;
        });

        // So this is the core logic of claiming jobs is here, and currently, it's just FIFO
        // Go through the queued jobs and start them if we have capacity
        // let index = 0;
        while (queuedJobKeys.length > 0) {
          const jobKey = queuedJobKeys.pop()!;
          if (this.queue[jobKey]) {
            continue;
          }
          // const job = jobStates[jobKey];
          const definition = await this.jobDefinitions.get(jobKey);
          if (!definition) {
            console.log(
              `${this.workerIdShort} ${getJobColorizedString(jobKey)} 🎳 _claimJobs failed to get definition`,
            );
            continue;
          }
          // (job.history[0].value as computeQueuesShared.StateChangeValueQueued)
          //   .definition;
          // Can I start this job?
          // This logic *could* be above in the while loop, but it's going to get
          // more complicated when we add more features, so make the check steps explicit
          // even if it's a bit more verbose
          const cpusOK = Object.keys(this.queue).length < this.cpus;

          if (cpusOK) {
            // cpu capacity is 👍
            // GPUs?
            if (definition.requirements?.gpus) {
              if (!this.isGPUCapacity()) {
                // no gpu capacity but the job needs it
                // skip this job
                if (config.debug) {
                  console.log(
                    `${this.workerIdShort} ${
                      getJobColorizedString(jobKey)
                    } 🎳 _claimJobs no gpu capacity but definition.gpu=[${definition.requirements?.gpus}]`,
                  );
                }
                continue;
              }
            }
            console.log(
              `${this.workerIdShort} ${getJobColorizedString(jobKey)} claiming job`,
            );
            this._startJob(jobKey, definition);
          } else {
            console.log(
              `${this.workerIdShort} ${getJobColorizedString(jobKey)} cannot claim job - no CPU capacity (${
                Object.keys(this.queue).length
              }/${this.cpus})`,
            );
          }
        }
      } while (this._needsAnotherClaimJobs);
    } finally {
      // Clear running flag when done
      this._isClaimingJobs = false;
    }
  }

  _startJob(jobId: string, definition: DockerJobDefinitionInputRefs): void {
    console.log(
      `${this.workerIdShort} ${getJobColorizedString(jobId)} starting...`,
    );
    // const queueBlob = jobBlob.history[0]
    //   .value as computeQueuesShared.StateChangeValueQueued;
    // const definition = queueBlob?.definition;

    const jobString = getJobColorizedString(jobId);
    if (!definition) {
      console.log(
        `${this.workerIdShort} 💥 _startJob but no this.jobs[${jobString}]`,
      );
      return;
    }

    // tell the server we've started the job
    const valueRunning: computeQueuesShared.StateChangeValueRunning = {
      type: computeQueuesShared.DockerJobState.Running,
      worker: this.workerId,
      time: Date.now(),
    };
    const runningMessageToServer: computeQueuesShared.WebsocketMessageWorkerToServer = {
      type: computeQueuesShared.WebsocketMessageTypeWorkerToServer.StateChange,
      payload: {
        job: jobId,
        tag: this.workerId,
        state: computeQueuesShared.DockerJobState.Running,
        value: valueRunning,
      },
    };

    // add a placeholder on the queue for this job
    this.queue[jobId] = {
      phase: DockerRunPhase.CopyInputs,
      time: valueRunning.time,
      execution: null,
      definition,
      runningMessageToServer,
    };
    let deviceRequests:
      | computeQueuesShared.DockerApiDeviceRequest[]
      | undefined;
    if (definition.requirements?.gpus) {
      const deviceIndex = this.getGPUDeviceIndex();
      this.queue[jobId].gpuIndices = [deviceIndex];
      deviceRequests = [{
        Driver: "nvidia",
        // Count: 1,
        DeviceIDs: [`${deviceIndex}`],
        Capabilities: [["gpu"]],
      }];

      // Log GPU allocation for manual validation
      console.log(
        `${this.workerIdShort} ${
          getJobColorizedString(jobId)
        } 🎮 GPU allocated: device=${deviceIndex} (from config: ${config.gpuConfig.originalSpec})`,
      );
    }

    this.sender(runningMessageToServer);

    // after this it can all happen async
    const isKilled: { value: boolean } = {
      value: false,
    };

    (async () => {
      if (isKilled.value) {
        return;
      }
      let volumes: Volume[];
      let outputsDir: string;
      try {
        if (!this.queue[jobId]) {
          return;
        }
        this.queue[jobId].phase = DockerRunPhase.CopyInputs;
        const volumesResult = await convertIOToVolumeMounts(
          { id: jobId, definition },
          config.server,
        );
        if (!this.queue[jobId]) {
          return;
        }
        this.queue[jobId].phase = DockerRunPhase.Building;
        volumes = volumesResult.volumes;
        outputsDir = volumesResult.outputsDir;
      } catch (err) {
        console.error(`${this.workerIdShort} 💥 `, err);
        // TODO too much code duplication here
        // Delete from our local queue before sending
        // TODO: cache locally before attempting to send
        delete this.queue[jobId];

        if (isKilled.value) {
          return;
        }

        const valueError: computeQueuesShared.StateChangeValueFinished = {
          type: computeQueuesShared.DockerJobState.Finished,
          reason: computeQueuesShared.DockerJobFinishedReason.Error,
          worker: this.workerId,
          time: Date.now(),
          result: {
            error: `${err}`,
            logs: [[`💥 ${err}`, Date.now(), true]],
          } as computeQueuesShared.DockerRunResultWithOutputs,
        };

        // console.log(
        //   `${this.workerIdShort} ${jobString} 🤡 SENDINGDockerJobState.Finished reason`,
        //   valueError.reason,
        // );

        this.sender({
          type: computeQueuesShared.WebsocketMessageTypeWorkerToServer.StateChange,
          payload: {
            job: jobId,
            tag: this.workerId,
            state: computeQueuesShared.DockerJobState.Finished,
            value: valueError,
          },
        });
        return;
      }

      if (isKilled.value) {
        return;
      }

      // TODO hook up the durationMax to a timeout
      // TODO add input mounts
      const workItem = this.queue[jobId];
      const executionArgs: DockerJobArgs = {
        workItem,
        workerId: this.workerId,
        sender: this.sender,
        queue: this.queueKey,
        id: jobId,
        image: definition.image,
        build: definition.build,
        command: definition.command ? convertStringToDockerCommand(definition.command, definition.env) : undefined,
        entrypoint: definition.entrypoint
          ? convertStringToDockerCommand(definition.entrypoint, definition.env)
          : undefined,
        workdir: definition.workdir,
        env: definition.env,
        shmSize: definition.shmSize,
        volumes,
        outputsDir,
        deviceRequests,
        maxJobDuration: definition?.maxDuration
          ? Math.min(
            parseDuration(definition?.maxDuration) as number,
            config.maxJobDuration,
          )
          : config.maxJobDuration,
        isKilled,
      };

      // Not awaiting, it should have already been created, but let's
      // check on every job anyway, but out of band
      ensureIsolateNetwork();

      const dockerExecution: DockerJobExecution = dockerJobExecute(
        executionArgs,
      );
      if (this.queue[jobId]) {
        this.queue[jobId].execution = dockerExecution;
      }
      if (!this.queue[jobId]) {
        console.log(
          `${this.workerIdShort} ${jobString} after await jobBlob.hash no job in queue so killing`,
        );
        // what happened? the job was removed from the queue by someone else?
        try {
          dockerExecution.kill("job removed from queue");
        } catch (err) {
          console.log(
            `${this.workerIdShort} ${jobString} ❗ dockerExecution.kill() errored but could be expeced`,
            err,
          );
        }

        return;
      }

      dockerExecution.finish.then(
        async (result: computeQueuesShared.DockerRunResult | undefined) => {
          if (!result) {
            console.log(
              `${this.workerIdShort} ${jobString} no result because killed`,
            );
            return;
          }
          console.log(
            `${this.workerIdShort} ${jobString} result ${JSON.stringify(result).substring(0, 100)}`,
          );
          result.logs = result.logs || [];
          if (result.StatusCode !== 0) {
            if (result.isTimedOut) {
              result.logs.push([
                "💥 Timed out",
                Date.now(),
                true,
              ]);
            }
            result.logs.push([
              `💥 StatusCode: ${result.StatusCode}`,
              Date.now(),
              true,
            ]);
            console.log(
              `${this.workerIdShort} ${jobString} 💥 StatusCode: ${result.StatusCode}`,
            );
            console.log(
              `${this.workerIdShort} ${jobString} 💥 stderr: ${result.logs?.join("\n")?.substring(0, 200)}`,
            );
          }
          if (result.error) {
            result.logs.push([`💥 ${result.error}`, Date.now(), true]);
            result.error = "Error";
            console.log(
              `${this.workerIdShort} ${jobString} 💥 error: ${result.error}`,
            );
          }

          const apiJob = this.apiQueue[jobId];
          if (!apiJob) {
            console.log(
              `${this.workerIdShort} ${jobString} ☠️ no apiJob found, ignoring job finished result`,
            );
            // delete this.queue[jobId];
            return;
          }
          // if another worker has grabbed our job, ignore our result
          if (
            apiJob.state === computeQueuesShared.DockerJobState.Running &&
            apiJob.worker !== this.workerId
          ) {
            console.log(
              `${this.workerIdShort} ${
                getJobColorizedString(jobId)
              } finished here, but running elsewhere, ignoring job`,
            );
            // delete this.queue[jobId];
            return;
          }
          if (
            apiJob.state === computeQueuesShared.DockerJobState.Finished &&
            apiJob.worker !== this.workerId
          ) {
            console.log(
              `${this.workerIdShort} ${jobString} finished here, but finished elsewhere, ignoring job`,
            );
            return;
          }

          const resultWithOutputs: computeQueuesShared.DockerRunResultWithOutputs =
            result as computeQueuesShared.DockerRunResultWithOutputs;
          resultWithOutputs.outputs = {};

          let valueFinished:
            | computeQueuesShared.StateChangeValueFinished
            | undefined;

          if (result.isTimedOut) {
            valueFinished = {
              type: DockerJobState.Finished,
              reason: computeQueuesShared.DockerJobFinishedReason.TimedOut,
              worker: this.workerId,
              time: Date.now(),
              result: resultWithOutputs,
            };
          } else if (result.error) {
            valueFinished = {
              type: DockerJobState.Finished,
              reason: computeQueuesShared.DockerJobFinishedReason.Error,
              worker: this.workerId,
              time: Date.now(),
              result: resultWithOutputs,
            };
          } else {
            // get outputs
            try {
              workItem.phase = DockerRunPhase.UploadOutputs;

              const outputs = await getOutputs(jobId, this.workerId);

              workItem.phase = DockerRunPhase.Ended;

              valueFinished = {
                type: DockerJobState.Finished,
                reason: computeQueuesShared.DockerJobFinishedReason.Success,
                worker: this.workerId,
                time: Date.now(),
                result: { ...result, outputs },
              };
            } catch (err) {
              workItem.phase = DockerRunPhase.Ended;
              console.log(
                `${this.workerIdShort} ${jobString} 💥 failed to upload outputs ${err}`,
              );
              resultWithOutputs.logs = resultWithOutputs.logs || [];
              console.error(err);
              resultWithOutputs.logs.push([
                `💥 failed to get job outputs`,
                Date.now(),
                true,
              ], [`err=${err}`, Date.now(), true]);
              valueFinished = {
                type: DockerJobState.Finished,
                reason: computeQueuesShared.DockerJobFinishedReason.Error,
                worker: this.workerId,
                time: Date.now(),
                result: { ...resultWithOutputs, error: `${err}` },
              };
            }
          }

          // Delete from our local queue first
          // TODO: cache locally before attempting to send
          workItem.phase = DockerRunPhase.Ended;
          delete this.queue[jobId];

          // console.log(
          //   `${this.workerIdShort} ${jobString} 🤡 SENDINGDockerJobState.Finished reason`,
          //   valueFinished.reason,
          // );

          this.sender({
            type: computeQueuesShared.WebsocketMessageTypeWorkerToServer
              .StateChange,
            payload: {
              job: jobId,
              tag: this.workerId,
              state: computeQueuesShared.DockerJobState.Finished,
              value: valueFinished,
            },
          });
        },
      ).catch((err: unknown) => {
        workItem.phase = DockerRunPhase.Ended;
        console.log(
          `${this.workerIdShort} ${jobString} 💥 errored ${err}`,
          (err as Error)?.stack,
        );

        // Delete from our local queue before sending
        // TODO: cache locally before attempting to send
        delete this.queue[jobId];

        const valueError: computeQueuesShared.StateChangeValueFinished = {
          type: DockerJobState.Finished,
          reason: computeQueuesShared.DockerJobFinishedReason.Error,
          worker: this.workerId,
          time: Date.now(),
          result: {
            error: err,
            logs: [[`💥 Job Error`, Date.now(), true], [
              `${err}`,
              Date.now(),
              true,
            ]],
          } as computeQueuesShared.DockerRunResultWithOutputs,
        };

        // console.log(
        //   `${this.workerIdShort} ${jobString} 🤡 SENDINGDockerJobState.Finished reason`,
        //   valueError.reason,
        // );

        this.sender({
          type: computeQueuesShared.WebsocketMessageTypeWorkerToServer.StateChange,
          payload: {
            job: jobId,
            tag: this.workerId,
            state: computeQueuesShared.DockerJobState.Finished,
            value: valueError,
          },
        });
      }).finally(() => {
        // I had the queue removal here initially but I wanted
        // to remove the element from the queue before sending to server
        // in case server send fails and throws an error
      });
    })();
  }

  /**
   * Kills the container and removes any local state
   */
  _killJobAndIgnore(locallyRunningJobId: string, reason?: string) {
    console.log(
      `${this.workerIdShort} ${getJobColorizedString(locallyRunningJobId)} Killing job (exists in our queue? ${!!this
        .queue[locallyRunningJobId]}) Reason: ${reason || "no reason provided"}`,
    );
    const localJob = this.queue[locallyRunningJobId];
    if (localJob?.execution) {
      localJob.execution.isKilled.value = true;
    }
    if (localJob) {
      localJob.phase = DockerRunPhase.Ended;
    }

    delete this.queue[locallyRunningJobId];
    localJob?.execution?.kill(reason || "killJobAndIgnore()");
  }

  private startRunningJobHealthInterval() {
    // Check every 10 seconds
    this.runningJobHealthInterval = setInterval(async () => {
      await this.checkRunningJobContainers();
    }, 10000);
  }

  public stopRunningJobHealthInterval() {
    if (this.runningJobHealthInterval) {
      clearInterval(this.runningJobHealthInterval);
      this.runningJobHealthInterval = null;
    }
  }

  private async checkRunningJobContainers() {
    for (const jobId of Object.keys(this.queue)) {
      const localJob = this.queue[jobId];
      // Only check jobs that are supposed to be running
      if (!localJob) continue;
      if ((Date.now() - localJob.time) < 5000) {
        console.log(
          `${this.workerIdShort} ${getJobColorizedString(jobId)} 🚨 Job is too new, skipping check.`,
        );
        continue;
      }
      // Defensive: only check jobs that are in the apiQueue and marked as running
      const apiJob = this.apiQueue[jobId];
      if (!apiJob || apiJob.state !== DockerJobState.Running) continue;
      if (!(localJob.phase === DockerRunPhase.Running || localJob.phase === DockerRunPhase.UploadOutputs)) continue;
      try {
        let container = localJob.execution?.container;
        if (!container) {
          container = await getRunningContainerForJob({ jobId, workerId: this.workerId });
          if (localJob.execution && !localJob.execution.container) {
            localJob.execution.container = container;
          }
        }
        if (!this.queue[jobId]) {
          continue;
        }
        if (!localJob?.execution?.container) {
          console.log(
            `${this.workerIdShort} ${
              getJobColorizedString(jobId)
            } 🚨 No running container found for job, killing job as orphaned.`,
          );
          this._killJobAndIgnore(jobId, "container missing or dead");
          continue;
        } else {
          try {
            const containerInfo = await localJob.execution.container.inspect();

            console.log(
              `${this.workerIdShort} ${
                getJobColorizedString(jobId)
              } 🚨  not sure what to do there containerInfo.State?.Status=${containerInfo.State?.Status}`,
            );
          } catch (err) {
            console.log(
              `${this.workerIdShort} ${getJobColorizedString(jobId)} 🚨 Error inspecting container for job:`,
              err,
            );
            this._killJobAndIgnore(jobId, "container missing or dead");
          }
        }
      } catch (err) {
        console.log(`${this.workerIdShort} ${getJobColorizedString(jobId)} 🚨 Error checking container for job:`, err);
      }
    }
  }

  /**
   * Kills jobs that belong to a different worker.
   */
  private async killOrphanedJobs() {
    console.log(`${this.workerIdShort} 🗑️ killing orphaned jobs`);

    const runningContainers = await docker.listContainers({
      filters: getDockerFiltersForJob({ status: "running" }),
    });
    for (const containerData of runningContainers) {
      const containerId = containerData.Id;
      try {
        const container = docker.getContainer(containerId);
        if (!container) {
          continue;
        }

        const containerInfo = await container.inspect();

        const workerId = containerInfo.Config?.Labels?.[ContainerLabelWorker];
        if (workerId && !workerId.startsWith("test-worker") && workerId !== this.workerId) {
          console.log(
            `🔥🔥🔥 killOrphanedJobs containerInfo from worker ${workerId} jobId=${
              getJobColorizedString(containerInfo.Config?.Labels?.[ContainerLabelId])
            }`,
          );
          container?.kill();
        }
      } catch (err) {
        /* do nothing */
        console.error(err);
      }
    }
  }
}
