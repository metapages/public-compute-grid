import { config, parseGpuSpec } from "/@/config.ts";
import { prepGpus, runChecksOnInterval } from "/@/docker/utils.ts";
import { localHandler } from "/@/lib/local-handler.ts";
import { processes, waitForDocker } from "/@/processes.ts";
import { clearCache } from "/@/queue/dockerImage.ts";
import { DockerJobQueue, type DockerJobQueueArgs } from "/@/queue/index.ts";
import humanizeDuration from "humanize-duration";
import { ms } from "ms";
import parseDuration from "parse-duration";
import ReconnectingWebSocket from "reconnecting-websocket";
import { ensureDir, existsSync } from "std/fs";
import { join } from "std/path";

import { Command } from "@cliffy/command";
import {
  type BroadcastJobStates,
  getJobColorizedString,
  getWorkerColorizedString,
  type PayloadClearJobCache,
  type WebsocketMessageSenderWorker,
  type WebsocketMessageServerBroadcast,
  WebsocketMessageTypeServerBroadcast,
  WebsocketMessageTypeWorkerToServer,
  type WebsocketMessageWorkerToServer,
} from "@metapages/compute-queues-shared";

import { getKv } from "../../../shared/src/shared/kv.ts";
import mod from "../../mod.json" with { type: "json" };
import { setGlobalDockerJobQueue } from "../cli.ts";
import { killAndRemoveContainerForJob } from "../queue/cleanup.ts";
import { JobDefinitionCache } from "../queue/JobDefinitionCache.ts";

const VERSION: string = mod.version;

const EnvPrefix = "METAPAGE_IO_WORKER_";

export const runCommand = new Command()
  .name("run")
  .arguments("[queue:string]")
  .description("Connect the worker to a queue")
  .option("-c, --cpus [cpus:number]", "Available CPU cores", { default: 1 })
  .option(
    "-a, --api-address [api-address:string]",
    "Custom API queue server",
  )
  .option("-g, --gpus [gpus:string]", "Available GPUs - specify number (e.g., '2') or devices (e.g., '\"device=1,3\"')")
  .option("-m, --mode [mode:string]", "Mode [default: remote]", {
    default: "remote",
  })
  .option("-p, --port [port:number]", "Port when mode=local", { default: 8000 })
  .option(
    "-d, --data-directory [dataDirectory:string]",
    "Data directory",
    { default: "/tmp/worker-metapage-io" },
  )
  .option("--id [id:string]", "Custom worker ID")
  .option(
    "-t, --max-job-duration [maxJobDuration:string]",
    "Maximum duration of a job. Default: 5m",
  )
  .option("--debug [debug:boolean]", "Debug mode", { default: undefined })
  .action(async (options, queue?: string) => {
    const METAPAGE_IO_WORKER_CPUS = Deno.env.get(`${EnvPrefix}CPUS`);
    config.cpus = typeof options.cpus === "number"
      ? options.cpus
      : (METAPAGE_IO_WORKER_CPUS ? parseInt(METAPAGE_IO_WORKER_CPUS) : 1);

    const METAPAGE_IO_WORKER_GPUS = Deno.env.get(`${EnvPrefix}GPUS`);
    const gpuSpec = (typeof options.gpus === "string" ? options.gpus : undefined) || METAPAGE_IO_WORKER_GPUS || "0";
    config.gpuConfig = parseGpuSpec(gpuSpec);
    config.gpus = config.gpuConfig.totalGpus;

    const METAPAGE_IO_WORKER_MODE = Deno.env.get(`${EnvPrefix}MODE`);
    config.mode = options.mode === "remote" || options.mode === "local"
      ? options.mode
      : (METAPAGE_IO_WORKER_MODE || "remote");

    const METAPAGE_IO_WORKER_QUEUE = Deno.env.get(`${EnvPrefix}QUEUE`);
    config.queue = config.mode === "local" ? "local" : queue || METAPAGE_IO_WORKER_QUEUE || "";
    if (!queue && !METAPAGE_IO_WORKER_QUEUE && config.mode === "remote") {
      throw new Error("Remote mode: must supply the queue id");
    }

    const METAPAGE_IO_WORKER_PORT = Deno.env.get(`${EnvPrefix}PORT`);
    config.port = typeof options.port === "number"
      ? options.port
      : (METAPAGE_IO_WORKER_PORT ? parseInt(METAPAGE_IO_WORKER_PORT) : 8000);

    config.dataDirectory = join(
      options.dataDirectory && typeof (options.dataDirectory) === "string"
        ? options.dataDirectory
        : "/tmp/worker-metapage-io",
      config.mode,
    );

    const METAPAGE_IO_WORKER_DEBUG = Deno.env.get(`${EnvPrefix}DEBUG`);
    config.debug = !!(typeof (options.debug) === "boolean" ? options.debug : METAPAGE_IO_WORKER_DEBUG === "true");

    console.log(`🔥 Setting DENO_KV_URL to ${join(config.dataDirectory, "kv")}`);
    Deno.env.set("DENO_KV_URL", join(config.dataDirectory, "kv"));
    console.log(`🔥 now? DENO_KV_URL ${Deno.env.get("DENO_KV_URL")}`);

    const METAPAGE_IO_WORKER_API_ADDRESS = Deno.env.get(`${EnvPrefix}API_ADDRESS`);
    config.server = typeof (options.apiAddress) === "string"
      ? options.apiAddress
      : (METAPAGE_IO_WORKER_API_ADDRESS ?? config.server);
    if (config.mode === "local") {
      config.server = `http://localhost:${config.port}`;
    }
    if (config.server.endsWith("/")) {
      config.server = config.server.slice(0, -1);
    }

    const METAPAGE_IO_WORKER_JOB_MAX_DURATION = Deno.env.get(
      `${EnvPrefix}JOB_MAX_DURATION`,
    );
    const stringDuration = typeof (options.maxJobDuration) === "string"
      ? options.maxJobDuration
      : (METAPAGE_IO_WORKER_JOB_MAX_DURATION || "5m");
    config.maxJobDuration = parseDuration(stringDuration) as number;

    if (options.id && typeof options.id === "string") {
      config.id = options.id;
      console.log(`🔥 Worker ID set from command line to ${config.id}`);
    } else {
      const kv = await getKv();
      const existingId: string | null = (await kv.get<string>(["workerId"]))
        ?.value;
      if (existingId) {
        config.id = existingId;
        console.log(`🔥 Worker ID set from kv to ${config.id}`);
      } else {
        config.id = crypto.randomUUID();
        console.log(`🔥 Worker ID generated because no id was set: ${config.id}`);
        await kv.set(["workerId"], config.id); // don't need to await
      }
      // If this is set, we are going to generate it every time
      if (Deno.env.get("METAPAGE_IO_WORKER_GENERATE_WORKER_ID")) {
        config.id = crypto.randomUUID();
        console.log(
          `🔥 Worker ID generated because METAPAGE_IO_WORKER_GENERATE_WORKER_ID=${
            Deno.env.get("METAPAGE_IO_WORKER_GENERATE_WORKER_ID")
          } is set: ${config.id}`,
        );
      }
    }

    console.log(
      `Worker config: [id=%s...] [queue=%s] [mode=%s] [cpus=%s] [gpus=%s] [gpuSpec=%s] [maxDuration=%s] [dataDirectory=%s] [api=%s] [debug=%s] ${
        config.mode === "local" ? "[port=%s]" : ""
      }`,
      config.id.substring(0, 12),
      config.queue,
      config.mode,
      config.cpus,
      config.gpus,
      config.gpuConfig.originalSpec,
      stringDuration,
      config.dataDirectory,
      config.server,
      config.debug,
      config.mode === "local" ? config.port : "",
    );

    // Check if we need to run in standalone mode
    if (Deno.env.get("METAPAGE_IO_WORKER_RUN_STANDALONE") === "true") {
      console.log(
        `Standalone mode: starting dockerd`,
      );
      (async () => {
        const dockerd = new Deno.Command("dockerd", {
          args: ["-p", "/var/run/docker.pid"],
        });
        processes.dockerd = dockerd.spawn();
        const result = await processes.dockerd.output();
        console.log("dockerd exited");
        console.log(result);
      })();
    }

    await waitForDocker();
    await prepGpus(config.gpus);
    await runChecksOnInterval(config.queue);

    if (config.mode === "local") {
      const cacheDir = join(config.dataDirectory, "f");
      await ensureDir(config.dataDirectory);
      await ensureDir(cacheDir);
      await Deno.chmod(config.dataDirectory, 0o777);
      await Deno.chmod(cacheDir, 0o777);
      console.log(
        `[mode=local] Data directory [${config.dataDirectory}] created ✅`,
      );
      console.log(`[mode=local] Cache directory [${cacheDir}] created ✅`);

      Deno.serve(
        {
          port: config.port,
          onError: (e: unknown) => {
            console.error(e);
            return new Response("Internal Server Error", { status: 500 });
          },
          onListen: ({ hostname, port }) => {
            console.log(
              `[mode=local] listening on hostname=${hostname} port=${port} 🚀 `,
            );

            // Once the server is listening, establish the connection
            connectToServer({
              server: config.server || "",
              queueId: config.queue,
              cpus: config.cpus,
              gpus: config.gpus ?? 0,
              workerId: config.id,
              port: config.port,
              maxJobDuration: stringDuration,
            });
          },
        },
        localHandler,
      );
    } else {
      connectToServer({
        server: config.mode === "local"
          // ? `http://localhost:${config.port}`
          ? `http://0.0.0.0:${config.port}`
          : config.server,
        queueId: config.queue,
        cpus: config.cpus,
        gpus: config.gpus,
        workerId: config.id,
        port: config.port,
        maxJobDuration: stringDuration,
      });
    }
  });

/**
 * Connect via websocket to the API server, and attach the DockerJobQueue object
 * TODO: listen to multiple job queues?
 */
export async function connectToServer(
  args: {
    server: string;
    queueId: string;
    cpus: number;
    gpus: number;
    workerId: string;
    port: number;
    maxJobDuration: string;
  },
) {
  const { server, queueId, cpus, gpus, workerId, port, maxJobDuration } = args;

  const url = config.mode === "local"
    ? `ws://localhost:${port}/q/${queueId}/worker`
    : `${server.replace("http", "ws")}/q/${queueId}/worker`;

  // @ts-ignore: frustrating cannot get compiler "default" import setup working
  console.log(`${getWorkerColorizedString(workerId)} 🪐 connecting... ${url}`);
  // @ts-ignore: frustrating cannot get compiler "default" import setup working
  const rws = new ReconnectingWebSocket(url, [], {
    maxReconnectionDelay: 6000,
    minReconnectionDelay: 1000 + Math.random() * 4000,
    reconnectionDelayGrowFactor: 1,
    minUptime: 5000,
    // connectionTimeout: 4000,
    connectionTimeout: 6000,
    maxRetries: Infinity,
    maxEnqueuedMessages: Infinity,
    debug: config.debug,
  });
  const sender: WebsocketMessageSenderWorker = (
    message: WebsocketMessageWorkerToServer,
  ) => {
    rws.send(JSON.stringify(message));
  };

  let timeLastPong = Date.now();
  let _timeLastPing = Date.now();
  let consecutivePingFailures = 0;
  let lastMessageReceived = Date.now();
  let connectionUptime = Date.now();

  const kv = await getKv();

  const jobDefinitionCache = new JobDefinitionCache({
    kv,
    sender,
  });

  const dockerJobQueueArgs: DockerJobQueueArgs = {
    sender,
    cpus,
    gpus,
    id: workerId,
    version: VERSION,
    time: Date.now(),
    maxJobDuration,
    queue: queueId,
    jobDefinitions: jobDefinitionCache,
  };
  const dockerJobQueue = new DockerJobQueue(dockerJobQueueArgs);
  setGlobalDockerJobQueue(dockerJobQueue);

  rws.addEventListener("error", (error: Error) => {
    console.log(`Websocket error=${error.message}`);
    consecutivePingFailures++;
    if (consecutivePingFailures > 3) {
      console.log(
        `${
          getWorkerColorizedString(workerId)
        } 🚨 Multiple websocket errors (${consecutivePingFailures}), forcing reconnect`,
      );
      new Promise((resolve) => setTimeout(resolve, 1000)).then(() => {
        rws.reconnect();
      });
    }
  });

  rws.addEventListener("open", () => {
    console.log(`${getWorkerColorizedString(workerId)} 🚀 connected! ${url} `);
    // This isn't a PING, but it's when we start measuring
    _timeLastPing = Date.now();
    timeLastPong = Date.now();
    lastMessageReceived = Date.now();
    connectionUptime = Date.now();
    consecutivePingFailures = 0;
    dockerJobQueue.register();
  });

  // let closed = false;
  // const pingInterval =
  setInterval(() => {
    if (rws.readyState === rws.OPEN) {
      // console.log("🌳 pinging");
      try {
        rws.send("PING");
        _timeLastPing = Date.now();
        // Reset failure counter on successful ping
        consecutivePingFailures = 0;
      } catch (err) {
        console.log(`🚨 Failed to send PING: ${err}`);
        consecutivePingFailures++;
      }
    } else {
      console.log("🌳 🚩 pinging but not open, state:", rws.readyState);
      consecutivePingFailures++;
    }
  }, 5000);

  rws.addEventListener("close", () => {
    // closed = true;
    // wsPool.remove(rws);
    console.log(
      `💥🚀💥 disconnected! ${url} after ${Date.now() - connectionUptime}ms uptime`,
    );
    // clearInterval(pingInterval);

    // Stop periodic registration when connection closes
    dockerJobQueue.stopPeriodicRegistration();
  });

  const intervalSinceNoTrafficToTriggerReconnect = ms("10s") as number;
  const reconnectCheckInterval = ms("3s") as number;
  const reconnectCheck = () => {
    const timeSinceLastPong = Date.now() - timeLastPong;
    const timeSinceLastMessage = Date.now() - lastMessageReceived;

    if (
      timeSinceLastPong >= intervalSinceNoTrafficToTriggerReconnect &&
      rws.readyState === rws.OPEN
    ) {
      console.log(
        `🚨 Reconnecting because no PONG since ${humanizeDuration(timeSinceLastPong)} >= ${
          humanizeDuration(intervalSinceNoTrafficToTriggerReconnect)
        }`,
      );
      rws.reconnect();
    } else if (
      timeSinceLastMessage >= (ms("30s") as number) &&
      rws.readyState === rws.OPEN
    ) {
      console.log(
        `🚨 No messages received for ${humanizeDuration(timeSinceLastMessage)}, reconnecting`,
      );
      rws.reconnect();
    }

    // Log connection health every minute
    // if ((Date.now() % 60000) < 3000) { // Every minute
    //   console.log(
    //     `📊 Connection health: uptime=${
    //       humanizeDuration(Date.now() - connectionUptime)
    //     }, ` +
    //       `lastPong=${humanizeDuration(timeSinceLastPong)}, ` +
    //       `lastMessage=${humanizeDuration(timeSinceLastMessage)}, ` +
    //       `pingFailures=${consecutivePingFailures}`,
    //   );
    // }

    setTimeout(reconnectCheck, reconnectCheckInterval);
  };
  setTimeout(reconnectCheck, reconnectCheckInterval);

  // const logGotJobStatesEvery = 10;
  // let currentGotJobStates = 0;
  // Liveness, not activity: this tracks when the socket last carried a
  // JobStates message, so it must start at "now" — starting at 0 makes the
  // watchdog below see ~56 years of silence and reconnect immediately.
  let timeSinceLastJobStates = Date.now();

  const twentySeconds = ms("20s") as number;
  setInterval(() => {
    if (Date.now() - timeSinceLastJobStates > twentySeconds) {
      console.log(
        `🚨 Reconnecting because no JobStates since ${humanizeDuration(Date.now() - timeSinceLastJobStates)} >= ${
          humanizeDuration(twentySeconds)
        }`,
      );
      rws.reconnect();
    }
  }, ms("10s") as number);

  rws.addEventListener("message", (message: MessageEvent) => {
    try {
      const messageString = message.data.toString();
      lastMessageReceived = Date.now(); // Update last message time

      if (messageString.startsWith("PONG")) {
        timeLastPong = Date.now();
        const pongedWorkerId = messageString.split(" ")[1];
        if (pongedWorkerId !== workerId) {
          console.log(
            `🚨 Server does not recognize us, registering again , sees [${pongedWorkerId}] expected [${workerId}]`,
          );
          dockerJobQueue.register();
          // } else {
          //   console.log(
          //     `🌳 ✅ PONG from server`,
          //   );
        }

        return;
      }

      if (!messageString.startsWith("{")) {
        if (config.debug) {
          console.log("➡️ 📧 to worker message not JSON", messageString);
        }
        return;
      }
      if (config.debug) {
        console.log("➡️ 📧 to worker message", messageString);
      }
      const possibleMessage: WebsocketMessageServerBroadcast = JSON.parse(
        messageString,
      );

      jobDefinitionCache.onWebsocketMessage(possibleMessage);

      switch (possibleMessage.type) {
        case WebsocketMessageTypeServerBroadcast.BroadcastJobDefinitions: {
          // handled by jobDefinitionCache.onWebsocketMessage
          break;
        }

        case WebsocketMessageTypeServerBroadcast.JobStates: {
          const allJobsStatesPayload = possibleMessage
            .payload as BroadcastJobStates;

          // Receiving the broadcast at all is the liveness signal, even when it
          // carries no jobs — an idle queue broadcasts `{jobs:{}}` steadily, and
          // only counting non-empty ones left the watchdog reconnecting forever
          // whenever nothing was running.
          timeSinceLastJobStates = Date.now();

          // if (Object.keys(allJobsStatesPayload?.state?.jobs || {}).length > 0) {
          //   console.log(
          //     `${getWorkerColorizedString(workerId)} JobStates from server: ${
          //       Object.keys(allJobsStatesPayload?.state?.jobs || {}).map((jobId) => getJobColorizedString(jobId)).join(
          //         ", ",
          //       )
          //       }`,
          //     );
          //   }

          // if (currentGotJobStates > logGotJobStatesEvery) {
          //   // console.log(
          //   //   `[${workerId?.substring(0, 6)}] got JobStates(${
          //   //     allJobsStatesPayload?.state?.jobs?.length || 0
          //   //   }) (only logging every ${logGotJobStatesEvery} messages)`,
          //   // );
          //   currentGotJobStates = 0;
          // }

          if (!allJobsStatesPayload) {
            console.log({
              error: "Missing payload in message",
              possibleMessage,
            });
            break;
          }
          dockerJobQueue.onUpdateSetAllJobStates(allJobsStatesPayload);
          break;
        }
        case WebsocketMessageTypeServerBroadcast.JobStateUpdates: {
          const someJobsPayload = possibleMessage.payload as BroadcastJobStates;

          // See the note in the JobStates case: any broadcast means the socket
          // is alive, empty payload included.
          timeSinceLastJobStates = Date.now();

          if (!someJobsPayload) {
            console.log({
              error: "Missing payload in message",
              possibleMessage,
            });
            break;
          }

          // const jobCount =
          //   Object.keys(someJobsPayload?.state?.jobs || {}).length;
          // console.log(
          //   `📥 Worker ${
          //     workerId?.substring(0, 6)
          //   } received JobStateUpdates with ${jobCount} jobs`,
          // );

          // if (currentGotJobStateUpdates > logGotJobStateUpdatesEvery) {
          //   console.log(
          //     `${getWorkerColorizedString(workerId)} got JobStateUpdates(${someJobsPayload?.state?.jobs?.length || 0})`,
          //   );
          //   currentGotJobStateUpdates = 0;
          // }

          dockerJobQueue.onUpdateUpdateASubsetOfJobs(someJobsPayload);
          break;
        }
        case WebsocketMessageTypeServerBroadcast.StatusRequest: {
          const status = dockerJobQueue.status();
          console.log(
            `${getWorkerColorizedString(workerId)} responding to status request with running jobs ${
              Object.keys(dockerJobQueue.queue).map((jobId) => getJobColorizedString(jobId)).join(", ")
            }`,
          );
          sender({
            type: WebsocketMessageTypeWorkerToServer.WorkerStatusResponse,
            payload: status,
          });
          break;
        }
        case WebsocketMessageTypeServerBroadcast.ClearJobCache: {
          const clearJobCacheConfirm = possibleMessage
            .payload as PayloadClearJobCache;
          const jobIdToClear = clearJobCacheConfirm.jobId;
          if (!jobIdToClear) {
            break;
          }
          console.log(
            `${getJobColorizedString(jobIdToClear)} 🗑️ deleting docker images`,
          );
          (async () => {
            const definition = await jobDefinitionCache.get(clearJobCacheConfirm.jobId);
            if (definition?.build) {
              clearCache({ build: definition.build });
            }
            await killAndRemoveContainerForJob({ jobId: jobIdToClear, workerId: config.id, queue: config.queue });
          })();

          const jobCacheDir = join(config.dataDirectory, "j", clearJobCacheConfirm.jobId);

          if (existsSync(jobCacheDir)) {
            try {
              console.log(
                `${getJobColorizedString(clearJobCacheConfirm?.jobId || "")} 🔥 deleting job cache dir ${jobCacheDir}`,
              );
              Deno.removeSync(jobCacheDir, { recursive: true });
            } catch (err) {
              console.log(
                `Error deleting job cache dir ${jobCacheDir}: ${err}`,
              );
            }
          }

          break;
        }
        default:
          if (config.debug) {
            console.log(
              `${getWorkerColorizedString(workerId || "")} unhandled message type: ${possibleMessage.type}`,
            );
          }
      }
    } catch (err) {
      console.log(`🚨 Error processing message: ${err}`);
    }
  });
}
