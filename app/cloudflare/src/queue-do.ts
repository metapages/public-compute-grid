/**
 * The queue's Durable Object, using the WebSocket Hibernation API so an idle
 * queue stops costing anything.
 *
 * Why it is shaped this way:
 *
 * - Hibernation means the object is evicted when nothing happens, and *any*
 *   pending timer prevents that (measured: a 5s repeating alarm kept the object
 *   resident across 20 alarms). So the queue is constructed with
 *   `periodicWork: false` and this object drives `maintenance()` from an alarm
 *   only while `hasWorkInFlight()` — a job queued, running, or recently
 *   finished and still to be evicted. Idle queue, no alarm, object sleeps.
 *
 * - A worker's PING cannot be auto-responded, even though that would keep it
 *   from waking us: the worker reads its own id back out of the PONG and
 *   re-registers when it is missing, and setWebSocketAutoResponse can only
 *   reply with a fixed string. The worker instead pings slowly while it is idle
 *   (see app/worker/src/commands/run.ts), which is what lets us sleep.
 *
 * - `BaseDockerJobQueue` attaches `addEventListener` to a socket and keeps
 *   per-connection state in those closures. Hibernation delivers messages to
 *   the object instead, and closures do not survive eviction. Rather than
 *   rewrite the queue for one runtime, each socket gets a `SocketShim` that
 *   looks like the WebSocket the queue expects, and the hibernation callbacks
 *   dispatch into it. The queue's own code is unchanged.
 *
 * - After an eviction the queue is rebuilt from the store, but a worker's
 *   registration lives only in memory, and a queue that thinks it has no
 *   workers can requeue their running jobs. So a worker's registration message
 *   is kept in its socket attachment and replayed through the queue's own
 *   handshake on wake — no new queue API, and the queue rebuilds its own state.
 */
import { DurableObject } from "cloudflare:workers";

import { getApiDockerJobQueue } from "@/routes/websocket.ts";
import type { ApiDockerJobQueue } from "@/docker-jobs/ApiDockerJobQueue.ts";
import { WebsocketMessageTypeWorkerToServer } from "@shared/types.ts";

/** How often maintenance runs while a queue has work in flight. */
const MAINTENANCE_INTERVAL_MS = 5_000;

type SocketRole = "worker" | "client";

interface SocketAttachment {
  role: SocketRole;
  queue: string;
  /** The worker's last registration message, verbatim, to replay after a wake. */
  registration?: string;
}

/**
 * The platform surface this object uses. Hand-declared, like the bindings in
 * worker.ts: the Cloudflare types are not among the ambient types here, and
 * pulling them in would add an npm tree to the Deno workspace.
 */
interface DurableObjectStorageLike {
  get<T>(key: string): Promise<T | undefined>;
  put(key: string, value: unknown): Promise<void>;
  getAlarm(): Promise<number | null>;
  setAlarm(scheduledTime: number): Promise<void>;
  deleteAlarm(): Promise<void>;
}

interface DurableObjectStateLike {
  storage: DurableObjectStorageLike;
  getWebSockets(tag?: string): HibernatableWebSocket[];
  acceptWebSocket(ws: WebSocket, tags?: string[]): void;
}

/** A WebSocket accepted for hibernation, which carries an attachment. */
type HibernatableWebSocket = WebSocket & {
  serializeAttachment(value: unknown): void;
  deserializeAttachment(): unknown;
  close(code?: number, reason?: string): void;
};

/**
 * Presents a hibernatable WebSocket as the event-target WebSocket the queue
 * expects. Only the surface the queue actually uses: addEventListener for
 * "message" and "close", send, readyState, close.
 */
class SocketShim {
  readonly socket: WebSocket;
  private messageListeners: ((event: MessageEvent) => void)[] = [];
  private closeListeners: (() => void)[] = [];

  constructor(socket: WebSocket) {
    this.socket = socket;
  }

  addEventListener(type: string, listener: (event: never) => void): void {
    if (type === "message") {
      this.messageListeners.push(listener as (event: MessageEvent) => void);
    } else if (type === "close") {
      this.closeListeners.push(listener as () => void);
    }
    // "error" and anything else: the hibernation callbacks report those on the
    // object itself, where they can be logged with the queue for context.
  }

  send(data: string): void {
    this.socket.send(data);
  }

  close(): void {
    this.socket.close();
  }

  get readyState(): number {
    return this.socket.readyState;
  }

  dispatchMessage(data: string): void {
    for (const listener of this.messageListeners) {
      listener({ data } as MessageEvent);
    }
  }

  dispatchClose(): void {
    for (const listener of this.closeListeners) {
      listener();
    }
  }
}

export interface QueueDurableObjectEnv {
  ASSETS: { fetch(request: Request | string): Promise<Response> };
}

export class QueueDurableObjectBase<Env extends QueueDurableObjectEnv> extends DurableObject<Env> {
  /**
   * The platform sets `ctx` on DurableObject, but its type is not visible here,
   * so narrow it once to the surface declared above instead of casting at every
   * use.
   */
  protected get doState(): DurableObjectStateLike {
    return (this as unknown as { ctx: DurableObjectStateLike }).ctx;
  }

  /** Shims for the sockets this instance has already attached to the queue. */
  private readonly shims = new Map<WebSocket, SocketShim>();
  private queue: ApiDockerJobQueue | undefined;
  private queueName: string | undefined;

  /**
   * The queue this object serves, rebuilt after an eviction. The name is kept
   * in storage because an alarm arrives with no request and no socket to read
   * it from.
   */
  protected async ensureQueue(nameFromRequest?: string): Promise<ApiDockerJobQueue | undefined> {
    if (!this.queueName) {
      this.queueName = nameFromRequest ?? await this.doState.storage.get<string>("queueName");
    }
    if (nameFromRequest && nameFromRequest !== this.queueName) {
      // One object per queue, so this should be impossible; say so rather than
      // quietly serve the wrong queue's state.
      console.error(
        `Durable Object for queue ${this.queueName} received a request for queue ${nameFromRequest}`,
      );
      this.queueName = nameFromRequest;
    }
    if (!this.queueName) {
      return undefined;
    }
    if (nameFromRequest) {
      await this.doState.storage.put("queueName", this.queueName);
    }
    if (!this.queue) {
      // Rehydrates the queue's jobs from the store; periodicWork: false because
      // a timer would keep this object resident (see the file comment).
      this.queue = await getApiDockerJobQueue(this.queueName, { periodicWork: false });
      await this.restoreSockets();
    }
    return this.queue;
  }

  /**
   * Re-attach the sockets that outlived the last eviction, replaying each
   * worker's registration so the queue knows its workers before it does any
   * maintenance.
   */
  private async restoreSockets(): Promise<void> {
    for (const socket of this.doState.getWebSockets()) {
      if (this.shims.has(socket)) {
        continue;
      }
      let attachment: SocketAttachment | undefined;
      try {
        attachment = (socket as HibernatableWebSocket).deserializeAttachment() as SocketAttachment | undefined;
      } catch (err) {
        console.error(
          `Could not read the attachment of a restored socket on queue ${this.queueName}, closing it:`,
          err,
        );
        (socket as HibernatableWebSocket).close(1011, "attachment unreadable");
        continue;
      }
      if (!attachment?.role) {
        console.error(
          `A restored socket on queue ${this.queueName} has no role attached, closing it`,
        );
        (socket as HibernatableWebSocket).close(1011, "no role attached");
        continue;
      }
      await this.attachSocket(socket, attachment);
    }
  }

  /** Hand a socket to the queue through a shim, as a worker or a client. */
  private async attachSocket(socket: WebSocket, attachment: SocketAttachment): Promise<void> {
    const queue = this.queue;
    if (!queue) {
      throw new Error(`Cannot attach a ${attachment.role} socket before the queue exists`);
    }
    const shim = new SocketShim(socket);
    this.shims.set(socket, shim);
    const asWebSocket = shim as unknown as WebSocket;
    if (attachment.role === "worker") {
      await queue.connectWorker({ socket: asWebSocket });
      if (attachment.registration) {
        // The queue learns its workers from their registration messages, so
        // replay the last one rather than wait up to 30s for the next.
        shim.dispatchMessage(attachment.registration);
      }
    } else {
      await queue.connectClient({ socket: asWebSocket });
    }
  }

  /** Attach a brand new socket: accepted hibernatable, role from the path. */
  protected async acceptSocket(
    socket: WebSocket,
    role: SocketRole,
    queueName: string,
  ): Promise<void> {
    // Build the queue first. ensureQueue re-attaches every socket the object
    // already has, and once this one is accepted it is one of them — doing it
    // the other way round attached it twice, so the queue saw two connections
    // for one socket.
    await this.ensureQueue(queueName);
    this.doState.acceptWebSocket(socket, [role]);
    const attachment: SocketAttachment = { role, queue: queueName };
    (socket as HibernatableWebSocket).serializeAttachment(attachment);
    await this.attachSocket(socket, attachment);
  }

  private async shimFor(socket: WebSocket): Promise<SocketShim | undefined> {
    await this.ensureQueue();
    const shim = this.shims.get(socket);
    if (shim) {
      return shim;
    }
    // ensureQueue restores every live socket, so a miss means this socket is
    // not one of ours (or was already closed).
    console.error(`A message arrived on a socket this object does not know (queue ${this.queueName})`);
    return undefined;
  }

  async webSocketMessage(socket: WebSocket, message: string | ArrayBuffer): Promise<void> {
    const text = typeof message === "string" ? message : new TextDecoder().decode(message);
    const shim = await this.shimFor(socket);
    if (!shim) {
      return;
    }
    this.rememberRegistration(socket, text);
    shim.dispatchMessage(text);
    await this.scheduleMaintenance();
  }

  /**
   * Keep a worker's registration in its attachment so it can be replayed after
   * an eviction. Parsing failures are the queue's to report, not ours: it
   * receives the same text immediately after.
   */
  private rememberRegistration(socket: WebSocket, text: string): void {
    if (!text.includes(WebsocketMessageTypeWorkerToServer.WorkerRegistration)) {
      return;
    }
    try {
      const parsed = JSON.parse(text) as { type?: string };
      if (parsed?.type !== WebsocketMessageTypeWorkerToServer.WorkerRegistration) {
        return;
      }
      const existing = (socket as HibernatableWebSocket).deserializeAttachment() as SocketAttachment | undefined;
      if (existing) {
        (socket as HibernatableWebSocket).serializeAttachment({ ...existing, registration: text });
      }
    } catch (err) {
      console.error(
        `Could not store a worker registration for replay on queue ${this.queueName} ` +
          `(the queue still handles the message itself):`,
        err,
      );
    }
  }

  async webSocketClose(socket: WebSocket, code?: number, reason?: string): Promise<void> {
    const shim = this.shims.get(socket);
    this.shims.delete(socket);
    // The queue drops the worker or client in its own close listener, which is
    // how a worker's jobs get requeued without waiting for a timer.
    shim?.dispatchClose();
    // Finish the closing handshake. Without this the peer never sees its own
    // close complete and waits forever; 1005/1006 are status codes that may not
    // be sent back, so they become a normal close.
    try {
      const sendableCode = code === undefined || code === 1005 || code === 1006 ? 1000 : code;
      (socket as HibernatableWebSocket).close(sendableCode, reason ?? "closed");
    } catch (err) {
      console.error(`Could not complete the closing handshake on queue ${this.queueName}:`, err);
    }
    await this.scheduleMaintenance();
  }

  async webSocketError(socket: WebSocket, error: unknown): Promise<void> {
    console.error(`WebSocket error on queue ${this.queueName}:`, error);
    await this.webSocketClose(socket, 1011, "socket error");
  }

  /**
   * An alarm only exists while there is work in flight, so this both does a
   * maintenance pass and decides whether to schedule the next one. No work
   * means no alarm, which is what lets the object hibernate.
   */
  async alarm(): Promise<void> {
    const queue = await this.ensureQueue();
    if (!queue) {
      console.error("Maintenance alarm fired for a Durable Object with no queue name stored");
      return;
    }
    try {
      await queue.maintenance();
    } catch (err) {
      console.error(`Maintenance failed for queue ${this.queueName}:`, err);
    }
    await this.scheduleMaintenance();
  }

  protected async scheduleMaintenance(): Promise<void> {
    const queue = this.queue;
    if (!queue) {
      return;
    }
    try {
      if (queue.hasWorkInFlight()) {
        if (await this.doState.storage.getAlarm() === null) {
          await this.doState.storage.setAlarm(Date.now() + MAINTENANCE_INTERVAL_MS);
        }
      } else if (await this.doState.storage.getAlarm() !== null) {
        await this.doState.storage.deleteAlarm();
      }
    } catch (err) {
      console.error(`Could not schedule maintenance for queue ${this.queueName}:`, err);
    }
  }
}
