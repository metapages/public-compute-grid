# Running workers

A worker is one process with access to a Docker daemon. Point it at a queue name and it starts taking jobs. Nothing
listens for inbound connections — the worker dials out — so laptops behind NAT work fine.

## The command

```sh
docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /tmp:/tmp \
  metapage/metaframe-docker-worker:0.54.87 run \
    --cpus=4 \
    --max-job-duration=20m \
    --data-directory=/tmp/worker-metapage-io \
    my-queue-name
```

The Docker socket mount is the point: the worker asks the _host_ daemon to run job containers as siblings, it does not
nest Docker. `/tmp` (or whatever `--data-directory` points at) holds the input/output/cache files, so mount it from the
host if you want the cache to survive a restart.

## Options

| Flag                     | Env var                                   | Default                   | Notes                                        |
| ------------------------ | ----------------------------------------- | ------------------------- | -------------------------------------------- |
| _(positional)_ queue     | `METAPAGE_IO_WORKER_QUEUE`                | —                         | Required in remote mode.                     |
| `-c, --cpus`             | `METAPAGE_IO_WORKER_CPUS`                 | `1`                       | Concurrency: CPUs this worker offers.        |
| `-g, --gpus`             | `METAPAGE_IO_WORKER_GPUS`                 | `0`                       | `2`, or `"device=1,3"` for specific devices. |
| `-m, --mode`             | `METAPAGE_IO_WORKER_MODE`                 | `remote`                  | `remote` or `local`.                         |
| `-t, --max-job-duration` | `METAPAGE_IO_WORKER_JOB_MAX_DURATION`     | `5m`                      | Hard ceiling per job on this machine.        |
| `-d, --data-directory`   | —                                         | `/tmp/worker-metapage-io` | Inputs, outputs, image and job cache.        |
| `-a, --api-address`      | `METAPAGE_IO_WORKER_API_ADDRESS`          | public API                | Point at your own deployment.                |
| `-p, --port`             | `METAPAGE_IO_WORKER_PORT`                 | `8000`                    | Local mode only.                             |
| `--id`                   | —                                         | persisted uuid            | Stable worker identity.                      |
| `--debug`                | `METAPAGE_IO_WORKER_DEBUG`                | off                       | Slower, much louder.                         |
| `--auto-update`          | `METAPAGE_IO_WORKER_AUTO_UPDATE`          | off                       | Upgrade to new releases, see below.          |
| `--auto-update-interval` | `METAPAGE_IO_WORKER_AUTO_UPDATE_INTERVAL` | `1h`                      | How often to look for a release.             |

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
    image: metapage/metaframe-docker-worker:0.54.87
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
  metapage/metaframe-docker-worker:0.54.87 run --cpus=8 --gpus=2 my-queue
```

Requires the NVIDIA container toolkit on the host. Jobs asking for `requirements.gpus` only go to workers with free
GPUs; the worker allocates a specific device per job and presents it to the container as index 0.

### Kubernetes / Nomad

There is nothing to coordinate — a worker is a stateless pod with the queue name in its args. Scale the deployment, or
drive the replica count from `GET /q/<queue>/status`. Examples live in `app/deploy/` in the repo.

## Upgrading automatically

```sh
docker run -d --restart unless-stopped --name worker \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /tmp:/tmp \
  metapage/metaframe-docker-worker:0.54.87 run --cpus=4 --auto-update my-queue
```

With `--auto-update` the worker checks for a newer release every `--auto-update-interval` (GitHub releases, falling back
to Docker Hub tags; pre-releases are ignored, it only moves forward) and replaces itself:

1. It asks the server first: `GET /api/v1/worker/versions` says which versions the server accepts, so a worker never
   runs ahead of the API it talks to.
2. It **pauses new jobs**. Jobs already running carry on: a worker never hands a running job over, because only the
   worker running it knows when it has finished and uploads its outputs.
3. It starts a successor container that is a copy of its own (`docker inspect`) with only the image tag changed: same
   args, env, mounts, network, restart policy, GPUs, privileged flag, labels, and the same worker id.
4. The successor connects in **standby**: registered on the queue, claiming nothing. The old worker waits until it sees
   it in `/q/<queue>/status`, and until its running jobs have finished.
5. With no jobs left, the old worker says it is ready. The successor stops and removes the old container, takes its
   name, and starts claiming.

While the old worker waits for its jobs, the queue gets no new work from it: other workers on the queue pick it up, or
it waits for the successor. If anything goes wrong before step 5 (the image cannot be pulled, the successor never
registers, it dies, a job is still running after `--max-job-duration`) the successor is removed, the old worker claims
jobs again, and it retries later (after 5 minutes, then doubling, never later than the next regular check). The new
worker is what stops the old one, so a failed upgrade never leaves the queue without a worker.

The worker must be able to see its own container through the mounted docker socket, so this does not apply to the
standalone image (its docker is its own), Kubernetes, or Fly/GCP-style deployments that recreate the worker from config.
A `deno compile`d worker binary downloads the new release, checks it runs, and re-execs in place. Local mode
(`--mode=local`) does not auto-update. Pin a fleet to a release with `METAPAGE_IO_WORKER_AUTO_UPDATE_VERSION=<version>`.

## Local mode

```sh
docker run -p 8000:8000 --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /tmp:/tmp \
  metapage/metaframe-docker-worker:0.54.87 run --mode=local --cpus=2
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
