# Quickstart

Everything below runs against the public API at `https://container.mtfm.io`. No account, no API key, no setup.

## 1. Pick a queue

A queue is just a name in a URL. It is created the first time something touches it, and it is private in exactly one
sense: **nobody can find it if nobody can guess it.**

```sh
QUEUE=my-$(uuidgen | tr 'A-Z' 'a-z')
echo $QUEUE
```

Use `public1` if you want to try things without running a worker — a small amount of shared compute is attached to it.

## 2. Submit a job

```sh
curl -s -X POST https://container.mtfm.io/q/$QUEUE \
  -H 'content-type: application/json' \
  -d '{
        "definition": {
          "image": "alpine:3.19.1",
          "command": "sh -c \"echo hello > /outputs/greeting.txt\""
        }
      }'
```

```json
{ "success": true, "jobId": "c0320fc230f9eaa0be00e138b05607e455a8d0a27a72dfa08c9c73fddf358f05" }
```

The `jobId` is the sha256 of the definition. Submit the same definition again and you get the same id — and, if it
already ran, the cached result.

## 3. Add compute

Nothing runs until a worker is attached to that queue. Any machine with Docker will do:

```sh
docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /tmp:/tmp \
  metapage/metaframe-docker-worker:0.54.83 run \
    --cpus=2 \
    --max-job-duration=20m \
    --data-directory=/tmp/worker-metapage-io \
    $QUEUE
```

Start it on a second machine and both work the same queue. That is the whole scaling story. See
[Running workers](/guide/workers).

## 4. Get the result

```sh
curl -s https://container.mtfm.io/q/$QUEUE/j/$JOB_ID/result.json
```

```json
{
  "data": {
    "state": "Finished",
    "finishedReason": "Success",
    "finished": {
      "result": {
        "StatusCode": 0,
        "duration": 184,
        "logs": [["stdout-line\n", 1784941344026]],
        "outputs": { "greeting.txt": { "type": "base64", "value": "aGVsbG8K" } }
      }
    }
  }
}
```

`{"data": null}` means "not finished yet" (or unknown job). Output files come back inline as base64 when they are small
and as a `{"type":"url"}` reference when they are big — or just fetch the raw bytes:

```sh
curl -sL https://container.mtfm.io/q/$QUEUE/j/$JOB_ID/outputs/greeting.txt
```

## 5. Wire it into a backend

Polling is one of three ways to find out a job finished. The full patterns — polling, websocket, and callback — with
copy-pasteable Node and Deno code are in [Backend integration](/guide/backend-integration).

## Other clients

**Browser** — <https://container.mtfm.io> is a full client. Configure an image and command, set the queue in the
bottom-right, hit _Run Job_, watch the terminal. The URL hash holds the whole job, so the link reproduces the run.

**CLI** — from a checkout of the repo:

```sh
cd app/cli
deno run --allow-all src/cli.ts job add $QUEUE \
  --file ../../README.md \
  -c 'sh -c "cat /inputs/README.md > /outputs/copy.md"' \
  --wait
```

**Build a container** — writing a Dockerfile and iterating until it runs is its own loop; see
[Building containers](/guide/building-containers).

**AI agent** — install the [Agent Skill](/guide/agent-skill) so your coding agent can build and debug a container on
this API, and knows the protocol without being told.
