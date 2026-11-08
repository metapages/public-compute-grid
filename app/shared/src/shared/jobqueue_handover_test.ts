/**
 * Two worker processes with the same id on one queue (#337): during an
 * --auto-update handover the successor connects with its predecessor's id.
 * Each socket gets its own entry, both show up in the status, and the
 * predecessor leaving does not make the id disappear.
 */
import { assertEquals } from "std/assert";

import { BaseDockerJobQueue, NoopBroadcastChannel, setBroadcastChannelFactory } from "./jobqueue.ts";
import {
  WebsocketMessageTypeServerBroadcast,
  WebsocketMessageTypeWorkerToServer,
  type WorkerHandoverState,
  type WorkerStatusResponse,
} from "./types.ts";

setBroadcastChannelFactory((name) => new NoopBroadcastChannel(name) as unknown as BroadcastChannel);

const WORKER_ID = "worker-handover";

/** A worker's socket, seen from the server: answers status requests like a worker. */
class FakeWorkerSocket extends EventTarget {
  readyState: number = WebSocket.OPEN;
  constructor(public version: string, public handover?: WorkerHandoverState) {
    super();
  }
  send(data: string) {
    if (data.startsWith("{") && JSON.parse(data).type === WebsocketMessageTypeServerBroadcast.StatusRequest) {
      const status: WorkerStatusResponse = {
        time: Date.now(),
        id: WORKER_ID,
        cpus: 1,
        gpus: 0,
        queue: {},
        version: this.version,
        handover: this.handover,
      };
      this.receive({ type: WebsocketMessageTypeWorkerToServer.WorkerStatusResponse, payload: status });
    }
  }
  receive(message: unknown) {
    this.dispatchEvent(new MessageEvent("message", { data: JSON.stringify(message) }));
  }
  register() {
    this.receive({
      type: WebsocketMessageTypeWorkerToServer.WorkerRegistration,
      payload: { id: WORKER_ID, version: this.version, cpus: 1, gpus: 0, time: Date.now(), maxJobDuration: "5m" },
    });
  }
  close() {
    this.readyState = WebSocket.CLOSED;
    this.dispatchEvent(new Event("close"));
  }
}

Deno.test("two sockets with one worker id: both registered, both in status, id survives the first leaving", async () => {
  const queue = new BaseDockerJobQueue({ serverId: "test", address: "handover-test", periodicWork: false });
  const predecessor = new FakeWorkerSocket("0.54.87");
  const successor = new FakeWorkerSocket("0.54.88", "standby");

  await queue.connectWorker({ socket: predecessor as unknown as WebSocket });
  predecessor.register();
  await queue.connectWorker({ socket: successor as unknown as WebSocket });
  successor.register();

  assertEquals(queue.getWorkerRegistrations().length, 2);
  assertEquals(queue.getWorkerRegistrations().map((r) => r.version), ["0.54.87", "0.54.88"]);

  const status = await queue.getStatusFromLocalWorkers();
  const workers = Object.values(status);
  assertEquals(workers.length, 2);
  assertEquals(
    workers.some((w) => w.id === WORKER_ID && w.version === "0.54.88" && w.handover === "standby"),
    true,
  );

  // re-registering on the same socket updates, it does not add
  successor.register();
  assertEquals(queue.getWorkerRegistrations().length, 2);

  predecessor.close();
  assertEquals(queue.getWorkerRegistrations().length, 1);
  assertEquals(queue.getWorkerRegistrations()[0].version, "0.54.88");
  assertEquals(queue.getWorkerRegistrations().map((r) => r.id), [WORKER_ID]);

  successor.close();
  assertEquals(queue.getWorkerRegistrations().length, 0);
});
