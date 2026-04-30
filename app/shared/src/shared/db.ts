import path from "node:path";

import { LRUMap } from "mnemonist";
import { retryAsync } from "retry";
import { ensureDir } from "std/fs";
import { join } from "std/path";
import { getKv } from "@shared/kv.ts";
import {
  type DataRef,
  DefaultNamespace,
  type DockerJobDefinitionInputRefs,
  DockerJobFinishedReason,
  DockerJobState,
  type EnqueueJob,
  type InMemoryDockerJob,
  type StateChange,
  type StateChangeValue,
  type StateChangeValueFinished,
} from "@shared/types.ts";
import { addJobProcessSubmissionWebhook } from "@shared/webhooks.ts";

import { JobDataCacheDurationMilliseconds } from "./constants.ts";
import {
  formatDefinitionS3Key,
  formatResultsS3Key,
  getJobColorizedString,
  getQueueColorizedString,
  isJobOkForSending,
  resolveMostCorrectJob,
  setJobStateFinished,
  setJobStateQueued,
  setJobStateRunning,
} from "./util.ts";

const AWS_SECRET_ACCESS_KEY = Deno.env.get("AWS_SECRET_ACCESS_KEY");

let deleteFromS3: (key: string) => Promise<void>;
let putJsonToS3: (key: string, data: unknown) => Promise<void>;
let getJsonFromS3: <T>(key: string) => Promise<T | undefined>;

const definitionCache = new LRUMap<string, DockerJobDefinitionInputRefs>(200);

/**
 *  /job/:jobId/<s3 key to definition.json>
 *  /job/:jobId/<s3 key to result.json>
 *  /job-queue-namespace/:jobId/:queue/:namespace/true
 *  /queue/:queue/:jobId/InMemoryJob.json
 */
export class DB {
  private kv: Deno.Kv;
  private dataDirectory: string;

  private constructor(kv: Deno.Kv, dataDirectory: string) {
    this.kv = kv;
    this.dataDirectory = dataDirectory;
  }

  static async initialize(dataDirectory?: string): Promise<DB> {
    const kv = await getKv();

    // Use the provided dataDirectory or default to TMPDIR
    const effectiveDataDirectory = dataDirectory || "/tmp/worker-metapage-io";

    if (!AWS_SECRET_ACCESS_KEY) {
      // Ensure the directory exists and has correct permissions if not using S3
      await ensureDir(effectiveDataDirectory);
      await Deno.chmod(effectiveDataDirectory, 0o777);
      await ensureDir(join(effectiveDataDirectory, "j"));
      await Deno.chmod(join(effectiveDataDirectory, "j"), 0o777);
      await ensureDir(join(effectiveDataDirectory, "f"));
      await Deno.chmod(join(effectiveDataDirectory, "f"), 0o777);
    }

    // Update S3 functions or local filesystem functions based on dataDirectory
    await DB.setupStorageFunctions(effectiveDataDirectory);

    return new DB(kv, effectiveDataDirectory);
  }

  private static async setupStorageFunctions(
    dataDirectory: string,
  ): Promise<void> {
    if (AWS_SECRET_ACCESS_KEY) {
      // Import S3 functions
      ({ deleteFromS3, putJsonToS3, getJsonFromS3 } = await import(
        "@shared/s3.ts"
      ));
    } else {
      deleteFromS3 = async (key: string): Promise<void> => {
        const filePath = join(dataDirectory, key);
        try {
          await Deno.remove(filePath);
        } catch (error) {
          if (error instanceof Deno.errors.NotFound) {
            // File already deleted, ignore
          } else {
            console.error(`Error deleting file ${filePath}:`, error);
          }
        }
      };

      putJsonToS3 = async (key: string, data: unknown): Promise<void> => {
        const filePath = join(dataDirectory, key);
        const dirPath = path.dirname(filePath);
        try {
          await Deno.mkdir(dirPath, { recursive: true, mode: 0o777 });
          const jsonData = JSON.stringify(data);
          await Deno.writeTextFile(filePath, jsonData, { mode: 0o777 });
        } catch (error) {
          console.error(`Error writing file ${filePath}:`, error);
          throw error;
        }
      };

      getJsonFromS3 = async <T>(
        key: string,
      ): Promise<T | undefined> => {
        const filePath = join(dataDirectory, key);
        try {
          const jsonData = await Deno.readTextFile(filePath);
          return JSON.parse(jsonData) as T;
        } catch (_) {
          // if (error instanceof Deno.errors.NotFound) {
          //   console.error(`File not found: ${filePath}`, (error as Error)?.message, (error as Error)?.stack);
          // } else {
          //   console.error(`Error reading file ${filePath}:`, (error as Error)?.message, (error as Error)?.stack);
          // }
          return undefined;
        }
      };
    }
  }

  // ... rest of the DB class methods ...

  // (The remaining methods of the DB class can remain unchanged)
  /**
   * Key spaces:
   *  - ["job", jobId]: s3 key to definition.json
   *  - ["job-results", jobId]: s3 key to results.json
   *  - ["job-queue-namespace", jobId, queue, namespace]: true if job is in queue+namespace
   *  - ["queue", queue, jobId]: in-memory job
   *  - ["queue-namespace-job-control", queue, jobId, namespace]: control object for job
   * @param queue
   * @param job
   */
  async queueJobAdd(args: {
    queue: string;
    job: EnqueueJob;
    inMemoryJob?: InMemoryDockerJob | null;
  }): Promise<InMemoryDockerJob | null> {
    let { queue, job, inMemoryJob: existingInMemoryJob } = args;
    const namespace = job?.control?.namespace || DefaultNamespace;
    const jobId = job.id;

    try {
      const existingNamespaces = await this.queueJobGetNamespaces({ queue, jobId });
      const includesNamespace = existingNamespaces.includes(namespace);

      const now = Date.now();

      if (!existingInMemoryJob) {
        existingInMemoryJob = await this.queueJobGet({ queue, jobId });
      }

      if (existingInMemoryJob) {
        const tentativeEnqueudJob = setJobStateQueued(existingInMemoryJob, { time: now });
        const mostCorrectJob = resolveMostCorrectJob(existingInMemoryJob, tentativeEnqueudJob);
        if (mostCorrectJob === existingInMemoryJob) {
          // no change, but update the namespace
          this.queueJobAddNamespace({
            queue,
            jobId,
            namespace,
          });
          const namespaces = await this.queueJobGetNamespaces({ queue, jobId });
          mostCorrectJob.namespaces = namespaces;
          return mostCorrectJob;
        }
      }
      // ok here means that the job *can* be set to Queued
      // but before that, we need to check if the job is finished

      const existingFinishedJob = await this.getFinishedJob(jobId);
      // console.log(`🌎 queueJobAdd existsingFinishedJob ${jobId}`, existingFinishedJob);
      if (existingFinishedJob) {
        const tentativeEnqueudJob = setJobStateQueued(existingFinishedJob, { time: now });
        const mostCorrectJob = resolveMostCorrectJob(existingFinishedJob, tentativeEnqueudJob);
        if (mostCorrectJob === existingFinishedJob) {
          // console.log(
          //   `🌎 queueJobAdd existsingFinishedJob same after resolving, adding namespace and returning it ${jobId}`,
          // );
          // add namespaces, queue, and return
          // no change, but update the namespace
          this.queueJobAddNamespace({
            queue,
            jobId,
            namespace,
          });
          const namespaces = await this.queueJobGetNamespaces({ queue, jobId });
          mostCorrectJob.namespaces = namespaces;
          return mostCorrectJob;
        }
      }

      // ok, no finished, or existing job, or we can overwrite

      const inMemoryJob: InMemoryDockerJob = {
        queuedTime: now,
        state: DockerJobState.Queued,
        time: now,
        worker: "",
        // this could be clobbered by a other
        debug: job.debug,
        namespaces: includesNamespace ? existingNamespaces : [...existingNamespaces, namespace],
        requirements: job.definition?.requirements,
        tags: job.definition?.tags,
      };

      const control = job?.control || {};
      const definitionS3Key = formatDefinitionS3Key(jobId);

      if (!existingFinishedJob && !definitionCache.has(definitionS3Key)) {
        await putJsonToS3(
          definitionS3Key,
          job.definition,
        );
        definitionCache.set(definitionS3Key, job.definition);
      }

      if (existingNamespaces.length === 0 && !existingFinishedJob) {
        await this.kv
          .atomic()
          .set(["job", jobId], definitionS3Key, { expireIn: JobDataCacheDurationMilliseconds })
          // if the value in "job-queue-namespace" is false, then the job is not
          // part of that namespace anymore
          .set(["job-queue-namespace", jobId, queue, namespace], true, {
            expireIn: JobDataCacheDurationMilliseconds,
          })
          .set(["queue", queue, jobId], inMemoryJob, {
            expireIn: JobDataCacheDurationMilliseconds,
          })
          .commit();

        // don't await this, it's not critical
        // this.appendToJobHistory({
        //   queue,
        //   jobId,
        //   value: {
        //     type: DockerJobState.Queued,
        //     time: Date.now(),
        //   },
        // });
      } else {
        await this.kv
          .atomic()
          .set(["job-queue-namespace", jobId, queue, namespace], true, {
            expireIn: JobDataCacheDurationMilliseconds,
          })
          .set(["queue", queue, jobId], inMemoryJob, {
            expireIn: JobDataCacheDurationMilliseconds,
          })
          .commit();
      }

      if (control) {
        // partition jobs that might be shared by the same namespace
        await this.kv.set(
          ["queue-job-namespace-control", queue, jobId, namespace],
          control,
          {
            expireIn: JobDataCacheDurationMilliseconds,
          },
        );
        await addJobProcessSubmissionWebhook({
          queue,
          namespace,
          jobId,
          control,
        });
      }

      return inMemoryJob;
    } catch (err) {
      console.error(
        `💥💥💥 ERROR adding job to queue ${
          getQueueColorizedString(
            queue,
          )
        } ${getJobColorizedString(jobId)}`,
        err,
      );
      throw err;
    }
  }

  async getJobDefinition(jobId: string): Promise<DockerJobDefinitionInputRefs | undefined> {
    const definitionKey = await this.kv.get<string | DataRef>(["job", jobId]);
    const value = definitionKey?.value;
    if (!value) {
      return undefined;
    }
    return getJsonFromS3(typeof value === "string" ? value : (value as DataRef)?.value);
  }

  async queueJobAddNamespace(args: {
    queue: string;
    jobId: string;
    namespace: string;
  }): Promise<void> {
    const { queue, jobId, namespace } = args;
    await this.kv.set(["job-queue-namespace", jobId, queue, namespace], true, {
      expireIn: JobDataCacheDurationMilliseconds,
    });
  }

  async queueJobRemoveNamespace(args: {
    jobId: string;
    queue: string;
    namespace: string;
  }): Promise<void> {
    const { queue, jobId, namespace } = args;
    // delete operations are not strongly consistent, so deletion followed by list
    // gives stale data, so we set to false and then delete
    await this.kv.set(["job-queue-namespace", jobId, queue, namespace], false, { expireIn: 1 });
    await this.kv.delete(["job-queue-namespace", jobId, queue, namespace]);
  }

  async queueJobGetNamespaces(args: {
    queue: string;
    jobId: string;
  }): Promise<string[]> {
    const { queue, jobId } = args;
    const result: string[] = [];
    try {
      const entries = this.kv.list<boolean>({
        prefix: ["job-queue-namespace", jobId, queue],
      }, { consistency: "strong" });
      for await (const entry of entries) {
        if (!entry.value || entry.versionstamp === null) {
          continue;
        }
        result.push(entry.key[3] as string);
      }
    } catch (err) {
      console.error(
        `Error in queueJobGetNamespaces for queue ${queue}, job ${jobId}:`,
        err,
      );
      throw err;
    }
    return result; //already sorted
  }

  async queueJobExists(
    args: { queue: string; jobId: string },
  ): Promise<boolean> {
    const job = await this.queueJobGet(args);
    return !!job;
  }

  async queueJobGet(
    args: { queue: string; jobId: string },
  ): Promise<InMemoryDockerJob | null> {
    const existingNamespaces = await this.queueJobGetNamespaces(args);
    const existingJob = await this.kv.get<InMemoryDockerJob>(["queue", args.queue, args.jobId]);
    if (!existingJob.value) {
      return null;
    }
    existingJob.value.namespaces = existingNamespaces;
    return existingJob.value;
  }

  async getJobFinishedResults(jobId: string): Promise<InMemoryDockerJob | undefined> {
    const resultsS3Key = formatResultsS3Key(jobId);
    const results: InMemoryDockerJob | undefined = await getJsonFromS3(resultsS3Key);
    return results;
  }

  async persistJobFinishedResults(args: {
    jobId: string;
    results: InMemoryDockerJob;
  }) {
    const { jobId, results } = args;
    const key = formatResultsS3Key(jobId);
    await putJsonToS3(key, results);
  }

  async deleteJobFinishedResults(jobId: string) {
    const key = formatResultsS3Key(jobId);
    await deleteFromS3(key);
  }

  /**
   * Sets this job finished on every queue it is on.
   * UH OH, it can be cancelled in one queue+namespace,
   * which should NOT impact the other queues+namespaces
   * but it currently does
   * Returns (optionally) the state change the calling queue
   * should apply
   */
  async setJobFinished(args: {
    queue: string;
    change: StateChangeValueFinished;
    jobId: string;
    job: InMemoryDockerJob;
  }): Promise<{ updatedInMemoryJob?: InMemoryDockerJob; subsequentStateChange?: StateChange | undefined }> {
    const { change, jobId, queue, job: existingJob } = args;

    // validate we can apply the change
    let mostCorrectJob = resolveMostCorrectJob(existingJob, setJobStateFinished(existingJob, { finished: change }));
    // console.log(
    //   `${getQueueColorizedString(queue)} ${getJobColorizedString(jobId)} 🌎 setJobFinished existingJob=${
    //     getJobStateString(existingJob)
    //   } mostCorrectJob=${getJobStateString(mostCorrectJob)}:`,
    // );

    // get all job-queues-namespaces for this job and update all the histories
    // ["job-queue-namespace", jobId, queue, namespace]
    // This is everywhere the job is.
    // only a few reasons are allowed to persist the results to s3
    switch (change.reason) {
      // RUNNING -> FINISHED (that)
      case DockerJobFinishedReason.Success:
      case DockerJobFinishedReason.Error:
      case DockerJobFinishedReason.TimedOut: {
        // no change, we are already up to date
        if (mostCorrectJob === existingJob) {
          return {};
        }

        // this stores both the big and small
        // job now has .finished removed because it's stored in s3
        mostCorrectJob = await this.setFinishedJob(jobId, mostCorrectJob);

        // NB: we store the finished state separate and pull it out when needed
        // because it's a large object, and we don't want to store it in deno kv
        // we don't need to do this, it's handled in setFinishedJob
        // delete job.finished; // this is added back by the clients or REST calls

        // get all queues this job is on
        const allQueues: Set<string> = new Set();
        const jobQueueNamespaces = this.kv.list<boolean>({
          prefix: ["job-queue-namespace", jobId],
        });
        for await (const entry of jobQueueNamespaces) {
          const queueEntry = entry.key[2]! as string;
          allQueues.add(queueEntry);
        }

        // update all the queues this job is on
        for await (const oneOfAllQueues of allQueues) {
          await this.kv.set(["queue", oneOfAllQueues, jobId], mostCorrectJob, {
            expireIn: JobDataCacheDurationMilliseconds,
          });
        }
        return { updatedInMemoryJob: mostCorrectJob };
      }

      // this job set back into queued state
      // RUNNING -> QUEUED
      case DockerJobFinishedReason.WorkerLost: {
        // no change, we are already up to date
        if (mostCorrectJob === existingJob) {
          return {};
        }
        // get all queues this job is on
        const allQueues: Set<string> = new Set();
        const jobQueueNamespaces = this.kv.list<boolean>({
          prefix: ["job-queue-namespace", jobId],
        });
        for await (const entry of jobQueueNamespaces) {
          const queueEntry = entry.key[2]! as string;
          allQueues.add(queueEntry);
        }

        mostCorrectJob = setJobStateQueued(mostCorrectJob, { time: change.time });

        // update all the queues this job is on
        for await (const oneOfAllQueues of allQueues) {
          await this.kv.set(["queue", oneOfAllQueues, jobId], mostCorrectJob, {
            expireIn: JobDataCacheDurationMilliseconds,
          });
          // don't await this, it's not critical
          // this.appendToJobHistory({
          //   queue,
          //   jobId,
          //   value: change,
          // });
        }

        return { updatedInMemoryJob: mostCorrectJob };
      }
      case DockerJobFinishedReason.Deleted: {
        // remove namespaces, then actually check if you can delete the job
        const namespaceChange = change.namespace || DefaultNamespace;
        let currentNamespaces = await this.queueJobGetNamespaces({ queue, jobId });

        // special handling of the wildcard (all) namespace, only used for cancelling
        if (change.namespace === "*") {
          // remove all namespaces
          for (const namespaceToRemove of currentNamespaces) {
            await this.queueJobRemoveNamespace({
              queue,
              jobId,
              namespace: namespaceToRemove,
            });
          }
          // jobs always stay in the default namespace
        } else if (currentNamespaces.includes(namespaceChange)) {
          await this.queueJobRemoveNamespace({
            queue,
            jobId,
            namespace: namespaceChange,
          });
        }
        currentNamespaces = await this.queueJobGetNamespaces({ queue, jobId });
        mostCorrectJob = {
          ...mostCorrectJob,
          namespaces: currentNamespaces,
        };
        // now if there are no namespaces left, we can actually delete the job
        console.log(
          `${getQueueColorizedString(queue)} ${getJobColorizedString(jobId)} 🌎 setJobFinished currentNamespaces:`,
          currentNamespaces,
        );
        if (currentNamespaces.length === 0) {
          console.log(`💥💥💥 deleting job ${jobId} from db`);
          await this.deleteFinishedJob(jobId);
          // await this.deleteJobFinishedResults(jobId);
          await this.deleteJobHistory({ queue, jobId });
          await this.kv.delete(["queue", queue, jobId]);
          return { updatedInMemoryJob: mostCorrectJob };
        } else {
          // other namespaces have a hold on this job, so we just update the namespaces
          const jobWithUpdatedNamespaces = { ...existingJob, namespaces: currentNamespaces };
          await this.kv.set(["queue", queue, jobId], jobWithUpdatedNamespaces, {
            expireIn: JobDataCacheDurationMilliseconds,
          });
          return { updatedInMemoryJob: jobWithUpdatedNamespaces };
        }
      }
      // these reasons: delete this version of the job by queue+namespace
      // RUNNING -> REMOVED for some and FINISHED for others
      case DockerJobFinishedReason.JobReplacedByClient: // ? JobReplacedByClient === Cancelled everywhere?
      case DockerJobFinishedReason.Cancelled: {
        const namespaceChange = change.namespace || DefaultNamespace;
        let currentNamespaces = await this.queueJobGetNamespaces({ queue, jobId });

        // special handling of the wildcard (all) namespace, only used for cancelling
        if (change.namespace === "*") {
          // remove all namespaces
          for (const namespaceToRemove of currentNamespaces) {
            await this.queueJobRemoveNamespace({
              queue,
              jobId,
              namespace: namespaceToRemove,
            });
          }
        } else if (currentNamespaces.includes(namespaceChange)) {
          await this.queueJobRemoveNamespace({
            queue,
            jobId,
            namespace: namespaceChange,
          });
        }
        currentNamespaces = await this.queueJobGetNamespaces({ queue, jobId });
        mostCorrectJob.namespaces = currentNamespaces;
        existingJob.namespaces = currentNamespaces;
        // now if there are no namespaces left, we can actually delete the job
        if (currentNamespaces.length === 0) {
          // this.appendToJobHistory({
          //   queue,
          //   jobId,
          //   value: change,
          // });
          await this.kv.set(["queue", queue, jobId], mostCorrectJob, {
            expireIn: JobDataCacheDurationMilliseconds,
          });

          return { updatedInMemoryJob: mostCorrectJob };
        } else {
          await this.kv.set(["queue", queue, jobId], existingJob, {
            expireIn: JobDataCacheDurationMilliseconds,
          });
          return { updatedInMemoryJob: existingJob };
        }
      }
      default:
        console.log(`💥💥💥 Unknown finished reason, no change: ${change.reason}`);
        return { updatedInMemoryJob: existingJob };
    }
  }

  // Sets this job running on every queue it is on
  async setJobRunning(args: {
    time: number;
    worker: string;
    jobId: string;
  }) {
    const { jobId } = args;

    // get all job queues and namespaces for this job and update all the histories
    // ["job-queue-namespace", jobId, queue, namespace]
    const entries = this.kv.list<boolean>({
      prefix: ["job-queue-namespace", jobId],
    });

    const queueDone = new Set<string>();

    for await (const entry of entries) {
      const jobId = entry.key[1]! as string;
      const queue = entry.key[2]! as string;

      if (queueDone.has(queue)) {
        continue;
      }
      queueDone.add(queue);

      let job = await this.queueJobGet({ queue, jobId });
      if (!job) {
        continue;
      }

      if (job.state === DockerJobState.Finished) {
        console.log(`${getJobColorizedString(jobId)} setJobRunning but job is finished, skipping`);
        continue;
      }

      job = setJobStateRunning(job, args);

      await this.kv.set(["queue", queue, jobId], job, {
        expireIn: JobDataCacheDurationMilliseconds,
      });

      // don't await this, it's not critical
      // this.appendToJobHistory({
      //   queue,
      //   jobId,
      //   value: runningStateChange,
      // });
    }
  }

  async getFinishedJob(jobId: string): Promise<InMemoryDockerJob | null> {
    const entry = await this.kv.get<InMemoryDockerJob>(["job-finished", jobId]);
    // console.log(`🌎 getFinishedJob ${jobId}`, entry.value);
    return entry.value;
  }

  async setFinishedJob(jobId: string, value: InMemoryDockerJob): Promise<InMemoryDockerJob> {
    await this.persistJobFinishedResults({ jobId, results: value });
    // the local job doesn't store the much bigger results
    if (value.finished) {
      value = { ...value };
      delete value.finished;
    }
    await this.kv.set(["job-finished", jobId], value, {
      expireIn: JobDataCacheDurationMilliseconds,
    });
    return value;
  }

  async deleteFinishedJob(jobId: string): Promise<void> {
    await this.kv.delete(["job-finished", jobId]);
    await this.deleteJobFinishedResults(jobId);
  }

  async getJobHistory(config: {
    queue: string;
    jobId: string;
  }): Promise<StateChangeValue[]> {
    const { queue, jobId } = config;
    const key = ["job-queue-history", queue, jobId];
    const currentList = await this.kv.get<StateChangeValue[]>(key);
    return currentList.value || [];
  }

  async deleteJobHistory(config: {
    queue: string;
    jobId: string;
  }): Promise<void> {
    const { queue, jobId } = config;
    const key = ["job-queue-history", queue, jobId];
    await this.kv.delete(key);
  }

  async appendToJobHistory(config: {
    queue: string;
    jobId: string;
    value: StateChangeValue;
  }) {
    let { queue, jobId, value } = config;
    const functionToMaybeRetry = async (): Promise<void> => {
      const key = ["job-queue-history", queue, jobId];

      if (value.type === DockerJobState.Finished) {
        value = {
          ...value,
        };
        // this can be large, and it's stored in s3
        delete (value as StateChangeValueFinished).result;
      }

      // Initial list (or get an existing one)
      const currentListFromDb = await this.kv.get<StateChangeValue[]>(key);
      const currentList = currentListFromDb.value || [];

      const result = await this.kv
        .atomic()
        // Check if the list hasn't been modified since we read it
        .check(currentListFromDb?.value ? currentListFromDb : { key, versionstamp: null })
        // Append the new item
        .set(key, [...currentList, value])
        .commit();

      if (!result.ok) {
        console.error("Failed to append item to list: optimistic lock failed");
        // Optionally implement retry logic or handle the conflict
      }
    };

    // try to do this 3 times, if it fails, throw an error.
    await retryAsync(functionToMaybeRetry, {
      maxTry: 3,
      delay: 1000,
    });
  }

  /**
   * No namespace? remove all namespaces
   * @param args
   */
  async queueJobRemove(
    args: { queue: string; namespace?: string; jobId: string },
  ): Promise<void> {
    const { queue, namespace, jobId } = args;
    // we don't need to delete the job definition, since it will expire anyway,
    // and some deletion tasks rely on referencing the original definition
    // if there is a namespace, we need to delete the namespace,
    // and conditionally delete the queue job only if there are no other namespaces
    // Also we don't delete default namespace jobs
    if (namespace && namespace !== DefaultNamespace) {
      await this.kv
        .atomic()
        .delete(["job-queue-namespace", jobId, queue, namespace])
        // probably don't actually delete "queue-job-namespace-control" since its stores callbacks
        // for job events, so other systems can track e.g. user + compute resources
        // .delete(["queue-job-namespace-control", queue, jobId, namespace])
        .commit();

      const namespaces = await this.queueJobGetNamespaces({ queue, jobId });
      if (namespaces.length === 0 || (namespaces.length === 1 && namespaces[0] === namespace)) {
        await this.kv.delete(["queue", queue, jobId]);
      }
    } else {
      const entries = this.kv.list<boolean>({
        prefix: ["job-queue-namespace", jobId, queue],
      });
      for await (const entry of entries) {
        const namespace = entry.key[3] as string;
        await this.kv
          .atomic()
          .delete(["job-queue-namespace", jobId, queue, namespace])
          // probably don't actually delete "queue-job-namespace-control" see above similar comment
          // .delete(["queue-job-namespace-control", queue, jobId, namespace])
          .commit();
      }
      await this.kv.delete(["queue", queue, jobId]);
    }
  }

  async queueGetAll(queue: string): Promise<Record<string, InMemoryDockerJob>> {
    try {
      const entries = this.kv.list<InMemoryDockerJob>({
        prefix: ["queue", queue],
      });
      const results: Record<string, InMemoryDockerJob> = {};
      for await (const entry of entries) {
        const jobId = entry.key[2] as string;
        results[jobId] = entry.value;
        if (results[jobId].state as string === "ReQueued") {
          results[jobId].state = DockerJobState.Queued;
        }
      }
      return results;
    } catch (err) {
      console.error(`Error in queueGetAll for queue ${queue}:`, err);
      throw err;
    }
  }

  async queueGetJobs(queue: string): Promise<Record<string, InMemoryDockerJob>> {
    const entries = this.kv.list<InMemoryDockerJob>({
      prefix: ["queue", queue],
    });
    const results: Record<string, InMemoryDockerJob> = {};
    for await (const entry of entries) {
      const jobId = entry.key[2] as string;
      const job = entry.value as InMemoryDockerJob;
      if (!isJobOkForSending(job)) {
        continue;
      }
      const namespaces = await this.queueJobGetNamespaces({ queue, jobId });
      job.namespaces = namespaces;
      results[jobId] = job;
      if (results[jobId].state as string === "ReQueued") {
        results[jobId].state = DockerJobState.Queued;
      }
    }
    return results;
  }

  async queueGetQueuedOrRunningCount(queue: string): Promise<number> {
    const entries = this.kv.list<InMemoryDockerJob>({
      prefix: ["queue", queue],
    });
    let count = 0;
    for await (const entry of entries) {
      const job = entry.value as InMemoryDockerJob;
      if (job.state === DockerJobState.Queued || job.state === DockerJobState.Running) {
        count++;
      }
    }
    return count;
  }
}
