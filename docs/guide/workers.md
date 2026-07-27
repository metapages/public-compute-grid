# Running workers

A worker is one process with access to a Docker daemon. Point it at a queue name and it starts taking jobs. Nothing
listens for inbound connections — the worker dials out — so laptops behind NAT work fine.

## The command

```sh
docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /tmp:/tmp \
  metapage/metaframe-docker-worker:0.54.83 run \
    --cpus=4 \
    --max-job-duration=20m \
    --data-directory=/tmp/worker-metapage-io \
    my-queue-name
```

The Docker socket mount is the point: the worker asks the _host_ daemon to run job containers as siblings, it does not
nest Docker. `/tmp` (or whatever `--data-directory` points at) holds the input/output/cache files, so mount it from the
host if you want the cache to survive a restart.

## Options

| Flag                     | Env var                               | Default                   | Notes                                        |
| ------------------------ | ------------------------------------- | ------------------------- | -------------------------------------------- |
| _(positional)_ queue     | `METAPAGE_IO_WORKER_QUEUE`            | —                         | Required in remote mode.                     |
| `-c, --cpus`             | `METAPAGE_IO_WORKER_CPUS`             | `1`                       | Concurrency: CPUs this worker offers.        |
| `-g, --gpus`             | `METAPAGE_IO_WORKER_GPUS`             | `0`                       | `2`, or `"device=1,3"` for specific devices. |
| `-m, --mode`             | `METAPAGE_IO_WORKER_MODE`             | `remote`                  | `remote` or `local`.                         |
| `-t, --max-job-duration` | `METAPAGE_IO_WORKER_JOB_MAX_DURATION` | `5m`                      | Hard ceiling per job on this machine.        |
| `-d, --data-directory`   | —                                     | `/tmp/worker-metapage-io` | Inputs, outputs, image and job cache.        |
| `-a, --api-address`      | `METAPAGE_IO_WORKER_API_ADDRESS`      | public API                | Point at your own deployment.                |
| `-p, --port`             | `METAPAGE_IO_WORKER_PORT`             | `8000`                    | Local mode only.                             |
| `--id`                   | —                                     | persisted uuid            | Stable worker identity.                      |
| `--debug`                | `METAPAGE_IO_WORKER_DEBUG`            | off                       | Slower, much louder.                         |

## Scaling

Concurrency is `--cpus`, and horizontal scale is "start another one":

```sh
# three machines, one queue — they share the work
ssh box-a 'docker run -d --restart unless-stopped … run --cpus=8 my-queue'
ssh box-b 'docker run -d --restart unless-stopped … run --cpus=8 my-queue'
ssh box-c 'docker run -d --restart unless-stopped … run --cpus=2 --gpus=1 my-queue'
```

Kill a worker mid-job and the job goes back to `Queued` for someone else (`WorkerLost`), so rolling restarts are safe.

### docker compose

```yaml
services:
  worker:
    image: metapage/metaframe-docker-worker:0.54.83
    restart: unless-stopped
    command: run --cpus=4 --max-job-duration=1h --data-directory=/data ${QUEUE}
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - worker-data:/data
volumes:
  worker-data:
```

### GPUs

```sh
docker run --rm --gpus all \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /tmp:/tmp \
  metapage/metaframe-docker-worker:0.54.83 run --cpus=8 --gpus=2 my-queue
```

Requires the NVIDIA container toolkit on the host. Jobs asking for `requirements.gpus` only go to workers with free
GPUs; the worker allocates a specific device per job and presents it to the container as index 0.

### Kubernetes / Nomad

There is nothing to coordinate — a worker is a stateless pod with the queue name in its args. Scale the deployment, or
drive the replica count from `GET /q/<queue>/status`. Examples live in `app/deploy/` in the repo.

## Local mode

```sh
docker run -p 8000:8000 --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /tmp:/tmp \
  metapage/metaframe-docker-worker:0.54.83 run --mode=local --cpus=2
```

The worker serves its own API on `http://localhost:8000` and runs everything locally — no cloud API, no blob uploads,
nothing leaves the machine. The queue is called `local`. Useful for development, air-gapped work, or sensitive data.

Same REST surface, so the code in [Backend integration](/guide/backend-integration) works unchanged against
`http://localhost:8000`.

## Is it working?

```sh
curl -s https://container.mtfm.io/q/$QUEUE/status | jq '{workers: .localWorkers | keys, jobs: .queueInfo.jobCount}'
```

If `workers` is empty, jobs will sit in `Queued` forever. Check the worker logs: it prints its id, the queue, and its
CPU/GPU counts on connect, and reconnects automatically when the API restarts.
