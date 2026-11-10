import { makeEnsureQueue } from "@metapages/compute-queues-shared";

import { ApiDockerJobQueue, userJobQueues } from "@/docker-jobs/ApiDockerJobQueue.ts";
import { getServerInstanceId } from "@/util/id.ts";

export interface WebsocketUrlParameters {
  token: string;
}

const makeEnsureApiQueue = (periodicWork: boolean | undefined) =>
  makeEnsureQueue(
    userJobQueues as { [queue: string]: ApiDockerJobQueue },
    // TODO: hydrate queue from some kind of persistence
    // actually the queue should handle that itself
    (queue) =>
      new ApiDockerJobQueue({
        serverId: getServerInstanceId(),
        address: queue,
        periodicWork,
      }),
  );

// One per periodicWork value, since create() only gets the queue name. A
// process only ever uses one of them: the Deno API never passes the option, a
// Cloudflare isolate always passes false. As before, a queue that already
// exists is returned whatever the option.
const ensureApiQueue = {
  default: makeEnsureApiQueue(undefined),
  noPeriodicWork: makeEnsureApiQueue(false),
};

/**
 * The queue for a name, created on first use. It is only published (and so
 * only handed to any caller) once its setup() has finished; concurrent first
 * calls share that setup.
 * https://github.com/metapages/public-compute-grid/issues/347
 */
export const getApiDockerJobQueue = (
  queue: string,
  // A Cloudflare Durable Object passes periodicWork: false and schedules
  // maintenance itself, because a timer would keep it resident and billed.
  opts?: { periodicWork?: boolean },
): Promise<ApiDockerJobQueue> =>
  opts?.periodicWork === false ? ensureApiQueue.noPeriodicWork(queue) : ensureApiQueue.default(queue);

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
    const jobQueue = await getApiDockerJobQueue(token);
    jobQueue.connectClient({ socket });
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
    const jobQueue = await getApiDockerJobQueue(token);
    jobQueue.connectWorker({ socket });
  } catch (err) {
    console.error(err);
  }
}
