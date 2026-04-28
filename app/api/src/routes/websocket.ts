import { ApiDockerJobQueue, userJobQueues } from "@/docker-jobs/ApiDockerJobQueue.ts";
import { SERVER_INSTANCE_ID } from "@/util/id.ts";

export interface WebsocketUrlParameters {
  token: string;
}

export const getApiDockerJobQueue = async (
  queue: string,
): Promise<ApiDockerJobQueue> => {
  if (!userJobQueues[queue]) {
    // TODO: hydrate queue from some kind of persistence
    // actually the queue should handle that itself
    const jobQueue = new ApiDockerJobQueue({
      serverId: SERVER_INSTANCE_ID,
      address: queue,
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
    if (!userJobQueues[token]) {
      // TODO: hydrate queue from some kind of persistence
      userJobQueues[token] = new ApiDockerJobQueue({
        serverId: SERVER_INSTANCE_ID,
        address: token,
      });
      await userJobQueues[token].setup();
    }
    userJobQueues[token].connectWorker({ socket });
  } catch (err) {
    console.error(err);
  }
}
