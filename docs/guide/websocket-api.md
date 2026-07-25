# WebSocket API

```
wss://container.mtfm.io/q/<queue>/client     ← clients (you)
wss://container.mtfm.io/q/<queue>/worker     ← workers (the worker binary)
```

One socket per queue, not per job. `browser` is accepted as a synonym for `client`.

## On connect

The server immediately sends, unprompted:

1. `Workers` — who is connected and what they have.
2. `JobStates` — a full snapshot of the queue's jobs.

So a client that connects after a job finished still learns about it.

## Server → client

| `type`                    | Payload                                                                 |
| ------------------------- | ----------------------------------------------------------------------- |
| `JobStates`               | `{ isSubset, state: { jobs: { [jobId]: InMemoryDockerJob } } }` — full. |
| `JobStateUpdates`         | Same shape, only changed jobs.                                          |
| `JobStatusPayload`        | `{ jobId, step, logs }` — live progress and log lines.                  |
| `Workers`                 | `{ workers: [{ id, cpus, gpus, version, maxJobDuration }] }`            |
| `BroadcastJobDefinitions` | `{ definitions: { [jobId]: definition } }`                              |

`InMemoryDockerJob`:

```ts
{
  state: "Queued" | "Running" | "Finished" | "Removed",
  time: number,
  queuedTime: number,
  worker: string,               // "" when unassigned
  finishedReason?: string,
  namespaces?: string[],
  requirements?: { cpus?, gpus?, memory?, maxDuration? },
}
```

::: warning State only — no results
Neither `JobStates` nor `JobStateUpdates` carries the result body. When you see `state === "Finished"`, fetch
`GET /q/<queue>/j/<jobId>/result.json`.
:::

`JobStatusPayload.step` is one of `docker image pull`, `cloning repo`, `docker build`, `Running`, `docker image push`.
`logs` are `[text, timestampMs, isStdErr?]` tuples — the same shape as in the final result, so one renderer handles live
and historical logs.

## Client → server

| `type`           | Payload                | Effect                                     |
| ---------------- | ---------------------- | ------------------------------------------ |
| `StateChange`    | A `StateChange` object | Submit a job (or drive its state).         |
| `QueryJob`       | `{ jobId }`            | Push me the current state of this one job. |
| `QueryJobStates` | —                      | Re-broadcast the full snapshot.            |

Plus the literal string `PING`, answered with `PONG`.

### Submitting over the socket

```js
socket.send(JSON.stringify({
  type: "StateChange",
  payload: {
    job: jobId, // sha256 of the definition
    tag: "",
    state: "Queued",
    value: {
      type: "Queued",
      time: Date.now(),
      enqueued: { id: jobId, definition, control },
    },
  },
}));
```

You have to compute the job hash yourself. `POST /q/<queue>` over REST does it for you and is otherwise equivalent —
submit over REST, listen on the socket.

### Handling the race

A job can finish before your socket is open. Ask explicitly rather than waiting for a delta that already happened:

```js
socket.addEventListener("open", () => {
  socket.send(JSON.stringify({ type: "QueryJob", payload: { jobId } }));
});
```

## Reconnecting

The socket is not durable — a dropped connection or a process restart loses the subscription. On reconnect:

1. Re-send `QueryJob` for every job you still care about (or read the connect-time `JobStates` snapshot).
2. Back off exponentially; the API redeploys periodically and every client reconnects at once.

The worker uses `reconnecting-websocket` for exactly this. If durability across restarts matters, persist `jobId`s and
reconcile with [polling](/guide/backend-integration#polling) on boot.

## Minimal client

```js
const socket = new WebSocket("wss://container.mtfm.io/q/my-queue/client");

socket.addEventListener("open", () => {
  socket.send(JSON.stringify({ type: "QueryJob", payload: { jobId } }));
  setInterval(() => socket.readyState === 1 && socket.send("PING"), 30_000);
});

socket.addEventListener("message", (event) => {
  if (event.data === "PONG") return;
  const { type, payload } = JSON.parse(event.data);
  if (type === "JobStates" || type === "JobStateUpdates") {
    const job = payload?.state?.jobs?.[jobId];
    if (job?.state === "Finished") { /* fetch result.json */ }
  }
});
```

A full example with logs and a promise wrapper is in [Backend integration](/guide/backend-integration#websocket).
