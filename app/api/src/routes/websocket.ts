import { holdSocketMessages, makeEnsureQueue } from "@metapages/compute-queues-shared";

import { ApiDockerJobQueue, userJobQueues } from "@/docker-jobs/ApiDockerJobQueue.ts";
import { getServerInstanceId } from "@/util/id.ts";

export interface WebsocketUrlParameters {
  token: string;
}

type ApiQueueOptions = { periodicWork?: boolean; serverId?: string };

const ensureApiQueue = makeEnsureQueue(
  userJobQueues as { [queue: string]: ApiDockerJobQueue },
  // TODO: hydrate queue from some kind of persistence
  // actually the queue should handle that itself
  (queue: string, opts?: ApiQueueOptions) =>
    new ApiDockerJobQueue({
      serverId: opts?.serverId ?? getServerInstanceId(),
      address: queue,
      periodicWork: opts?.periodicWork,
    }),
);

/**
 * The queue for a name, created on first use. It is only published (and so
 * only handed to any caller) once its setup() has finished; concurrent first
 * calls share that setup, and only the call that builds the queue has its
 * opts applied. As before, a queue that already exists is returned whatever
 * the opts.
 * https://github.com/metapages/public-compute-grid/issues/347
 */
export const getApiDockerJobQueue = (
  queue: string,
  // A Cloudflare Durable Object passes periodicWork: false and schedules
  // maintenance itself, because a timer would keep it resident and billed. It
  // also passes its own serverId, one per queue instance (see queue-do.ts).
  opts?: ApiQueueOptions,
): Promise<ApiDockerJobQueue> => ensureApiQueue(queue, opts);

export async function wsHandlerClient(
  token: string,
  socket: WebSocket,
  _request: Request,
) {
  // const server:FastifyInstanceWithDB = this as FastifyInstanceWithDB;

  try {
    // console.log(`/client/:token wsHandler`)

    // console.log('token', token);
    if (!token || token === "" || token === "undefined" || token === "null") {
      console.log("No token, closing socket");
      console.log(`🐋 ws: closing and returning because invalid key: ${token}`);
      socket.close();
      return;
    }
    const releaseMessages = holdSocketMessages(socket);
    const jobQueue = await getApiDockerJobQueue(token);
    jobQueue.connectClient({ socket });
    releaseMessages();
  } catch (err) {
    console.error(err);
  }
}

export async function wsHandlerWorker(
  token: string,
  socket: WebSocket,
  _request: Request,
) {
  try {
    if (!token) {
      console.log("No token/queue, closing socket");
      socket.close();
      return;
    }
    const releaseMessages = holdSocketMessages(socket);
    const jobQueue = await getApiDockerJobQueue(token);
    jobQueue.connectWorker({ socket });
    releaseMessages();
  } catch (err) {
    console.error(err);
  }
}
