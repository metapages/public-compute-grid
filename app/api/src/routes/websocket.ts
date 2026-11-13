import { holdSocketMessages } from "@metapages/compute-queues-shared";
import { ApiDockerJobQueue, userJobQueues } from "@/docker-jobs/ApiDockerJobQueue.ts";
import { getServerInstanceId } from "@/util/id.ts";

export interface WebsocketUrlParameters {
  token: string;
}

export const getApiDockerJobQueue = async (
  queue: string,
  // A Cloudflare Durable Object passes periodicWork: false and schedules
  // maintenance itself, because a timer would keep it resident and billed. It
  // also passes its own serverId, one per queue instance (see queue-do.ts).
  opts?: { periodicWork?: boolean; serverId?: string },
): Promise<ApiDockerJobQueue> => {
  if (!userJobQueues[queue]) {
    // TODO: hydrate queue from some kind of persistence
    // actually the queue should handle that itself
    const jobQueue = new ApiDockerJobQueue({
      serverId: opts?.serverId ?? getServerInstanceId(),
      address: queue,
      periodicWork: opts?.periodicWork,
    });
    await jobQueue.setup();
    userJobQueues[queue] = jobQueue;
    return jobQueue;
  }
  return userJobQueues[queue];
};

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
    if (!userJobQueues[token]) {
      // TODO: hydrate queue from some kind of persistence
      userJobQueues[token] = new ApiDockerJobQueue({
        serverId: getServerInstanceId(),
        address: token,
      });
      await userJobQueues[token].setup();
    }
    userJobQueues[token].connectWorker({ socket });
    releaseMessages();
  } catch (err) {
    console.error(err);
  }
}
