/**
 * A worker that reconnects (it missed a PONG) registers on its new socket
 * before the server sees its old socket close (#343). The server has to keep
 * the worker: losing it leaves a worker that is connected, answered by PONGs
 * carrying its own id, and never given another job.
 */
import { assertEquals, assertStrictEquals } from "std/assert";

import {
  BaseDockerJobQueue,
  type CollectedWorkersRegistration,
  NoopBroadcastChannel,
  setBroadcastChannelFactory,
} from "./jobqueue.ts";
import type { DB } from "./db.ts";
import { WebsocketMessageTypeWorkerToServer, type WorkerRegistration } from "./types.ts";

setBroadcastChannelFactory((name) => new NoopBroadcastChannel(name) as unknown as BroadcastChannel);

/** Enough of a WebSocket for connectWorker: events in, and whatever it sends */
class FakeSocket extends EventTarget {
  readyState: number = WebSocket.OPEN;
  sent: string[] = [];
  send(message: string) {
    this.sent.push(message);
  }
  receive(data: string) {
    this.dispatchEvent(new MessageEvent("message", { data }));
  }
  close() {
    this.readyState = WebSocket.CLOSED;
    this.dispatchEvent(new Event("close"));
  }
}

const registration = (id: string): WorkerRegistration => ({
  version: "test",
  id,
  cpus: 2,
  gpus: 0,
  time: Date.now(),
  maxJobDuration: "5m",
});

const register = (socket: FakeSocket, id: string) =>
  socket.receive(JSON.stringify({
    type: WebsocketMessageTypeWorkerToServer.WorkerRegistration,
    payload: registration(id),
  }));

/** The workers this server holds a socket for */
const myWorkers = (queue: BaseDockerJobQueue): CollectedWorkersRegistration["myWorkers"] =>
  // deno-lint-ignore no-explicit-any
  (queue as any).workers.myWorkers;

const createQueue = () => {
  const queue = new BaseDockerJobQueue({ serverId: "test", address: "workers-test", periodicWork: false });
  queue.db = {
    queueJobGet: () => Promise.resolve(null),
    getFinishedJob: () => Promise.resolve(null),
  } as unknown as DB;
  return queue;
};

Deno.test("a worker that reconnects stays registered when its old socket closes after the new one registers", async () => {
  const queue = createQueue();
  try {
    const oldSocket = new FakeSocket();
    await queue.connectWorker({ socket: oldSocket as unknown as WebSocket });
    register(oldSocket, "worker-1");
    assertEquals(myWorkers(queue).length, 1);

    // reconnect: the new socket registers first, then the old one closes
    const newSocket = new FakeSocket();
    await queue.connectWorker({ socket: newSocket as unknown as WebSocket });
    register(newSocket, "worker-1");
    oldSocket.close();

    assertEquals(myWorkers(queue).length, 1, "the reconnected worker was removed");
    assertStrictEquals(
      myWorkers(queue)[0].connection,
      newSocket as unknown as WebSocket,
      "jobs would be sent to the closed socket",
    );

    // and closing the socket it is actually on still removes it
    newSocket.close();
    assertEquals(myWorkers(queue).length, 0);
  } finally {
    queue.dispose();
  }
});

Deno.test("a worker's old socket closing does not remove a different worker", async () => {
  const queue = createQueue();
  try {
    const socket1 = new FakeSocket();
    const socket2 = new FakeSocket();
    await queue.connectWorker({ socket: socket1 as unknown as WebSocket });
    await queue.connectWorker({ socket: socket2 as unknown as WebSocket });
    register(socket1, "worker-1");
    register(socket2, "worker-2");

    socket1.close();

    assertEquals(myWorkers(queue).map((w) => w.registration.id), ["worker-2"]);
  } finally {
    queue.dispose();
  }
});
