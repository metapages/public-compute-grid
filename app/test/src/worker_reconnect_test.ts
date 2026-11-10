import { assert, assertEquals } from "std/assert";

import { closed, open } from "@korkje/wsi";
import {
  fetchRobust,
  WebsocketMessageTypeServerBroadcast,
  WebsocketMessageTypeWorkerToServer,
  type WorkerRegistration,
} from "@metapages/compute-queues-shared";
import { closeKv } from "../../shared/src/shared/kv.ts";

const fetch = fetchRobust;

const QUEUE_ID = Deno.env.get("QUEUE_ID") || "local1";
const API_URL = Deno.env.get("API_URL") ||
  (QUEUE_ID === "local" ? "http://worker:8000" : "http://api1:8081");

const isWorkerOnQueue = async (workerId: string): Promise<boolean> => {
  const response = await fetch(`${API_URL}/q/${QUEUE_ID}/status`);
  const status: { localWorkers?: Record<string, unknown> } = await response.json();
  return !!status.localWorkers?.[workerId];
};

const waitForWorkerOnQueue = async (workerId: string, expected: boolean) => {
  for (let attempt = 0; attempt < 50; attempt++) {
    if (await isWorkerOnQueue(workerId) === expected) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  assertEquals(await isWorkerOnQueue(workerId), expected);
};

const connectAndRegister = async (workerId: string): Promise<WebSocket> => {
  const socket = new WebSocket(`${API_URL.replace("http", "ws")}/q/${QUEUE_ID}/worker`);
  await open(socket);
  // no cpus, so this worker can never claim a job from the other tests
  const registration: WorkerRegistration = {
    time: Date.now(),
    version: "test",
    id: workerId,
    cpus: 0,
    gpus: 0,
    maxJobDuration: "1h",
  };
  socket.send(JSON.stringify({
    type: WebsocketMessageTypeWorkerToServer.WorkerRegistration,
    payload: registration,
  }));
  // The status route only lists the workers that answer it
  socket.addEventListener("message", (event) => {
    if (`${event.data}`.includes(WebsocketMessageTypeServerBroadcast.StatusRequest)) {
      socket.send(JSON.stringify({
        type: WebsocketMessageTypeWorkerToServer.WorkerStatusResponse,
        payload: { time: Date.now(), id: workerId, cpus: 0, gpus: 0, queue: {} },
      }));
    }
  });
  return socket;
};

Deno.test("A worker that reconnects is not removed when its old socket closes", async () => {
  const workerId = `test-reconnect-${crypto.randomUUID()}`;

  const oldSocket = await connectAndRegister(workerId);
  await waitForWorkerOnQueue(workerId, true);

  // A reconnect: the server can see the new socket before the old one's close
  const newSocket = await connectAndRegister(workerId);
  await new Promise((resolve) => setTimeout(resolve, 500));
  oldSocket.close();
  await closed(oldSocket);
  await new Promise((resolve) => setTimeout(resolve, 500));

  assert(await isWorkerOnQueue(workerId), "the old socket closing removed the reconnected worker");

  newSocket.close();
  await closed(newSocket);
  await waitForWorkerOnQueue(workerId, false);
  closeKv();
});
