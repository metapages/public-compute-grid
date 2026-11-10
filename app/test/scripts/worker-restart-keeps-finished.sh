#!/usr/bin/env bash
# #358 end to end: a worker restarted between finishing a job and the server
# learning of it must send the result it already has, not run the job again.
#
#   worker-restart-keeps-finished.sh <api url, http://...> [worker dir]
#
# A remote-mode worker from this checkout (or [worker dir], e.g. a checkout of
# main as a control) connects to the API through a TCP proxy. While its job
# runs the proxy is frozen: the connection stays open, so the server keeps the
# worker and the job Running on it, but nothing gets through. The job finishes,
# its Finished is stuck in the socket's send queue, and the worker is killed.
# A new worker process with the same id and data directory then connects
# through a fresh proxy and sees "Running on me" for a job it has no local
# entry for. Each run prints a fresh uuid, so the stored result shows which run
# the server got.
set -euo pipefail

api="${1:?usage: $0 <http api url> [worker dir]}"
api="${api%/}"
worker_dir="${2:-$(cd "$(dirname "$0")/../../worker" && pwd)}"
case "$api" in http://*) ;; *) echo "the api url must be http:// (the proxy is plain TCP)" >&2; exit 1 ;; esac
hostport="${api#http://}"

queue="restart-$(date +%s)-$RANDOM"
id="restart-e2e-${queue}"
data="$(mktemp -d)"
marker="e2e-$RANDOM$RANDOM"
pids=()

cleanup() {
    for pid in "${pids[@]}"; do
        pkill -CONT -P "$pid" 2> /dev/null || true
        kill -CONT "$pid" 2> /dev/null || true
        pkill -9 -P "$pid" 2> /dev/null || true
        kill -9 "$pid" 2> /dev/null || true
    done
    docker ps -aq --filter "label=workerId=${id}" | xargs docker rm -f > /dev/null 2>&1 || true
    echo "logs kept in ${data}"
}
trap cleanup EXIT

free_port() { deno eval 'const l = Deno.listen({ port: 0 }); console.log(l.addr.port); l.close();'; }

# Both set $started to the new pid (not echoed: a command substitution would
# wait for the background process holding its stdout)
start_proxy() {
    local port="$1"
    socat "TCP-LISTEN:${port},fork,reuseaddr" "TCP:${hostport}" > /dev/null 2>&1 &
    started=$!
    pids+=("$started")
}

start_worker() {
    local port="$1" log="$2"
    (cd "$worker_dir" && exec deno run --allow-all --unstable-kv --unstable-cron src/cli.ts run \
        --cpus=1 --id="$id" --data-directory="$data" --max-job-duration=5m \
        --api-address="http://localhost:${port}" "$queue" > "$log" 2>&1) &
    started=$!
    pids+=("$started")
}

wait_for() {
    local what="$1" seconds="$2"
    shift 2
    for _ in $(seq 1 "$seconds"); do
        if "$@"; then return 0; fi
        sleep 1
    done
    echo "💥 timed out waiting for ${what}" >&2
    exit 1
}

job_state() { curl -fsS "${api}/q/${queue}/status" | jq -r --arg j "$job" '.jobs[$j].state // empty'; }
worker_listed() { curl -fsS "${api}/q/${queue}/status" | grep -q "$id"; }
job_running() { [ "$(job_state)" = "Running" ]; }
container_done() { grep -q "container finished" "$data/worker-a.log"; }
result_ready() { [ "$(curl -fsS "${api}/q/${queue}/j/${job}/result.json" | jq '.data != null')" = "true" ]; }

echo "queue=${queue} worker=${id} data=${data}"

proxy_a_port="$(free_port)"
start_proxy "$proxy_a_port"
proxy_a=$started
start_worker "$proxy_a_port" "$data/worker-a.log"
worker_a=$started
wait_for "worker A to register" 60 worker_listed

job="$(curl -fsS -X POST "${api}/q/${queue}" -H 'content-type: application/json' \
    -d "{\"definition\":{\"image\":\"alpine:3.18.5\",\"command\":\"sh -c 'sleep 8; echo ${marker}-run-\$(cat /proc/sys/kernel/random/uuid)'\"}}" |
    jq -r .jobId)"
echo "job=${job}"
wait_for "the job to run" 60 job_running
sleep 2

echo "🧊 freezing the proxy: the server keeps the connection, nothing reaches it"
pkill -STOP -P "$proxy_a" || true
kill -STOP "$proxy_a"
wait_for "the job's container to finish" 60 container_done
sleep 2
echo "🔪 killing worker A with its Finished unsent"
pkill -9 -P "$worker_a" || true
kill -9 "$worker_a"
[ "$(job_state)" = "Running" ] || { echo "💥 the server already has state $(job_state), the scenario did not happen" >&2; exit 1; }

proxy_b_port="$(free_port)"
start_proxy "$proxy_b_port"
start_worker "$proxy_b_port" "$data/worker-b.log"
wait_for "the job's result" 90 result_ready

result="$(curl -fsS "${api}/q/${queue}/j/${job}/result.json")"
runs_b="$(grep -c "container started" "$data/worker-b.log" || true)"
echo "result reason: $(echo "$result" | jq -r '.data.reason // .data.finished.reason // empty')"
echo "worker B started containers: ${runs_b}"
grep -E "resending Finished|jobsServerSaysAreRunningOnMe, starting" "$data/worker-b.log" | head -5 || true

if [ "$runs_b" != "0" ]; then
    echo "❌ the restarted worker ran the finished job a second time" >&2
    exit 1
fi
echo "✅ the restarted worker resent the result it had instead of running the job again"
