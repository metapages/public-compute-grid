#!/usr/bin/env bash
# #361: a job's container that exits while its worker is down is collected by
# the restarted worker (same id, same data directory), not run again.
#
# Starts its own local-mode worker (it is the API server too) on a free port,
# runs a job that writes an output, logs a line, sleeps and exits 3, kills the
# worker with SIGKILL while the job runs, waits for the container to exit, then
# starts the worker again. Passes if the job's result has exit code 3, the
# first run's log line and output, and only one container was ever created.
#
# Usage: worker-restart-collects-exited.sh [worker dir]   (default: app/worker)
# Needs docker, jq and curl.
set -euo pipefail

worker_dir="$(cd "${1:-$(dirname "$0")/../../worker}" && pwd)"
nonce="$(date +%s)-$RANDOM"
# test-worker*: other workers on this docker host leave its containers alone (killOrphanedJobs)
worker_id="test-worker-restart-exited-${nonce}"
data_dir="$(mktemp -d)"
port=$((30000 + RANDOM % 9000))
while lsof -nP -iTCP:"$port" -sTCP:LISTEN > /dev/null 2>&1; do port=$((port + 1)); done
api="http://localhost:${port}"
worker_pid=""

cleanup() {
    [ -n "$worker_pid" ] && kill -9 "$worker_pid" 2> /dev/null || true
    containers="$(docker ps -aq --filter "label=container.mtfm.io/worker=${worker_id}")"
    [ -n "$containers" ] && docker rm -fv $containers > /dev/null 2>&1 || true
    [ -n "${events_pid:-}" ] && kill "$events_pid" 2> /dev/null || true
    rm -rf "$data_dir"
}
trap cleanup EXIT

fail() {
    echo "❌ $*" >&2
    echo "--- worker log (after restart) ---" >&2
    tail -40 "$data_dir/worker-b.log" >&2 2> /dev/null || true
    exit 1
}

start_worker() {
    (cd "$worker_dir" && exec deno run --allow-all --unstable-kv --unstable-cron src/cli.ts run \
        --mode=local --cpus=2 --gpus=0 --port="$port" \
        --data-directory="$data_dir/data" --id="$worker_id" > "$data_dir/$1" 2>&1 < /dev/null) &
    worker_pid=$!
    for _ in $(seq 1 120); do
        curl -fsS --max-time 2 "$api/healthz" > /dev/null 2>&1 && return 0
        sleep 1
    done
    fail "worker did not answer on $api"
}

job_state() {
    curl -fsS "$api/q/local/j" | jq -r --arg id "$1" '.data[$id].state // empty'
}

# Count container creations for this worker id, across the restart
docker events --filter "label=container.mtfm.io/worker=${worker_id}" --filter event=create \
    --format '{{.Actor.ID}}' > "$data_dir/creates.txt" &
events_pid=$!

echo "worker ${worker_id} on ${api} (data ${data_dir})"
start_worker worker-a.log

definition=$(jq -n --arg nonce "$nonce" '{
    image: "alpine:3.20",
    command: ("sh -c \"echo first-run-" + $nonce + "; echo from-the-first-run > /outputs/result.txt; sleep 8; exit 3\"")
}')
job_id=$(curl -fsS -X POST -H 'Content-Type: application/json' \
    -d "$(jq -n --argjson definition "$definition" '{definition: $definition}')" \
    "$api/q/local/j" | jq -r .jobId)
echo "job ${job_id}"

for _ in $(seq 1 120); do
    [ -n "$(docker ps -q --filter "label=container.mtfm.io/worker=${worker_id}" --filter status=running)" ] && break
    sleep 1
done
container="$(docker ps -q --filter "label=container.mtfm.io/worker=${worker_id}" --filter status=running)"
[ -n "$container" ] || fail "the job's container never started"
echo "container ${container} running, killing the worker"
kill -9 "$worker_pid"
wait "$worker_pid" 2> /dev/null || true
worker_pid=""

echo "waiting for the container to exit while the worker is down"
exit_code="$(docker wait "$container")"
[ "$exit_code" = "3" ] || fail "container exited with $exit_code, expected 3"

echo "restarting the worker with the same id and data directory"
start_worker worker-b.log

for _ in $(seq 1 60); do
    [ "$(job_state "$job_id")" = "Finished" ] && break
    sleep 1
done
[ "$(job_state "$job_id")" = "Finished" ] || fail "job not Finished after the restart (state: $(job_state "$job_id"))"

result="$(curl -fsS "$api/q/local/j/${job_id}/result.json")"
status_code="$(echo "$result" | jq -r '.. | objects | select(has("StatusCode")) | .StatusCode' | head -1)"
[ "$status_code" = "3" ] || fail "result StatusCode=$status_code, expected 3: $(echo "$result" | head -c 600)"
echo "$result" | grep -q "first-run-${nonce}" || fail "result is missing the first run's log line"
output="$(curl -fsS "$api/q/local/j/${job_id}/outputs/result.txt")"
[ "$output" = "from-the-first-run" ] || fail "output result.txt is '$output'"

grep -q "container started" "$data_dir/worker-b.log" && fail "the restarted worker started a container: the job ran again"
creates="$(sort -u "$data_dir/creates.txt" | grep -c . || true)"
[ "$creates" = "1" ] || fail "$creates containers were created for the job, expected 1"
for _ in $(seq 1 10); do
    [ -z "$(docker ps -aq --filter "id=${container}")" ] && break
    sleep 1
done
[ -z "$(docker ps -aq --filter "id=${container}")" ] || fail "the collected container was not removed"

echo "✅ not rerun: exit code 3, the first run's logs and output, 1 container (removed once reported)"
