set shell := ["bash", "-c"]
set dotenv-load := true
set export := true
set quiet := true

APP_PORT := env_var_or_default("APP_PORT", "443")
normal := '\033[0m'
green := "\\e[32m"
cyan := "\\e[36m"

@_help:
    echo ""
    just --list --unsorted --list-heading $'Commands: (all services)\n'
    echo -e ""
    echo -e "    Sub-commands (e.g. just browser dev):"
    echo -e "       {{ green }}app{{ normal }}            -> just app"
    echo -e "       {{ green }}browser{{ normal }}        -> just app/browser"
    echo -e "       {{ green }}worker{{ normal }}         -> just app/worker"
    echo -e "       {{ green }}api{{ normal }}            -> just app/api"
    echo -e "       {{ green }}docs{{ normal }}           -> just docs"
    echo -e ""
    echo -e "    Current worker version: {{ cyan }}$(cat app/worker/mod.json | jq -r '.version'){{ normal }}"
    echo -e ""
    echo -e "    Quick links:"
    echo -e "       api local:             {{ green }}https://worker-metaframe.localhost:{{ APP_PORT }}/{{ normal }}"
    echo -e "       api production:        {{ green }}https://container.mtfm.io{{ normal }}"
    echo -e "       github repo:           {{ green }}https://github.com/metapages/public-compute-grid{{ normal }}"
    echo -e "       api deployment config: {{ green }}https://dash.cloudflare.com/?to=/:account/workers/services/view/compute-queues{{ normal }}"

# Validate mode
@_validate_mode mode="":
    @if [ "{{ mode }}" = "remote" ] || [ "{{ mode }}" = "local" ] || [ "{{ mode }}" = "cloudflare" ]; then :; else echo "Error: Mode must be 'remote', 'local' or 'cloudflare'" >&2; exit 1; fi

# Start Development Environment
@dev mode="remote" +args="": (_validate_mode mode)
    just app/dev {{ mode }} {{ args }}

# Runs All Functional Tests and checks code (for the mode specified)
@test mode="remote":
    just app test {{ mode }}

# Runs All Functional Tests and checks code
test-all:
    just app test-all

# API contract tests against any deployment (e.g. staging), with a temporary local worker on a fresh queue
test-url url:
    #!/usr/bin/env bash
    set -euo pipefail
    queue="contract-$(date +%s)-$RANDOM"
    data_dir="$(mktemp -d)"
    echo -e "Testing {{ url }} on queue ${queue}"
    (cd app/worker && deno run --allow-all --unstable-kv --unstable-cron src/cli.ts run \
        --cpus=2 --id="test-url-${queue}" --data-directory="${data_dir}" \
        --api-address={{ url }} "${queue}" > "${data_dir}/worker.log" 2>&1) &
    worker_pid=$!
    trap 'kill ${worker_pid} 2>/dev/null || true; pkill -f "test-url-${queue}" 2>/dev/null || true' EXIT
    for i in $(seq 1 60); do
        if curl -sf "{{ url }}/q/${queue}/status" | grep -q "test-url-${queue}"; then break; fi
        sleep 1
    done
    cd app/test && QUEUE_ID="${queue}" API_URL={{ url }} deno test --allow-all --unstable-kv src/contract_api_test.ts

# target: worker (local-mode worker on :8000, queue "local" — the default) |
# stack (the full compose stack, queue "local1") | prod (no redirect).
# Symlinked, not copied, so edits are live; cq is pointed at the local stack so
# an agent using it cannot submit to the shared production queue.

# Symlink the Agent Skill into your skills dir, aimed at a local stack
dev-install-skill target="worker" dir="~/.claude/skills":
    #!/usr/bin/env bash
    set -euo pipefail
    dest="${dir/#\~/$HOME}"
    src="{{ justfile_directory() }}/docs/public/skill/compute-queues"
    [ -f "$src/SKILL.md" ] || { echo "No SKILL.md at $src" >&2; exit 1; }

    case "$target" in
        worker) api="http://localhost:8000"; queue="local"
                hint="just app/worker/local" ;;
        stack)  api="https://worker-metaframe.localhost:${APP_PORT:-443}"; queue="local1"
                hint="just dev" ;;
        prod)   api=""; queue="" ;;
        *) echo "Unknown target: $target (expected worker, stack or prod)" >&2; exit 1 ;;
    esac

    # cq reads dev-target.json next to itself. It is gitignored, so it never
    # reaches a published release — for installed users the defaults stay
    # production.
    if [ -n "$api" ]; then
        cat > "$src/scripts/dev-target.json" <<JSON
    {
      "api": "$api",
      "queue": "$queue",
      "source": "just dev-install-skill $target"
    }
    JSON
        echo -e "  {{ green }}dev target{{ normal }} $api (queue $queue)"
        if ! curl -fsS --max-time 3 "$api/healthz" >/dev/null 2>&1; then
            echo -e "  {{ cyan }}note{{ normal }} nothing answering there yet — start it with: $hint"
        fi
    else
        rm -f "$src/scripts/dev-target.json"
        echo -e "  {{ cyan }}dev target cleared{{ normal }} — cq will use production defaults"
    fi

    mkdir -p "$dest"
    # Replace whatever is there: a stale symlink, or a copy left by install.sh.
    # Say so when it's a real directory being replaced — that is a curl-installed
    # release quietly disappearing.
    if [ -d "$dest/compute-queues" ] && [ ! -L "$dest/compute-queues" ]; then
        echo -e "  {{ cyan }}replacing{{ normal }} the installed copy at $dest/compute-queues with a link to this repo"
    fi
    rm -rf "$dest/compute-queues"
    ln -s "$src" "$dest/compute-queues"
    echo -e "  {{ green }}linked{{ normal }} $dest/compute-queues -> $src"
    echo -e "Restart your agent so it reloads SKILL.md."

# Only unlinks a symlink, so a real (curl-installed) skill dir is never deleted.

# Undo dev-install-skill
dev-uninstall-skill dir="~/.claude/skills":
    #!/usr/bin/env bash
    set -euo pipefail
    target="${dir/#\~/$HOME}"
    rm -f "{{ justfile_directory() }}/docs/public/skill/compute-queues/scripts/dev-target.json"
    if [ -L "$target/compute-queues" ]; then
        rm "$target/compute-queues"
        echo -e "  {{ green }}unlinked{{ normal }} $target/compute-queues"
    elif [ -e "$target/compute-queues" ]; then
        echo -e "  {{ cyan }}skipped{{ normal }} $target/compute-queues (a real directory, not a symlink)"
    else
        echo "Nothing linked at $target/compute-queues"
    fi

# Needs a local-mode worker, which serves the API too: just app/worker/local

# Test the compute-queues skill's API surface (no LLM)
test-skill queue="local" api="http://localhost:8000":
    #!/usr/bin/env bash
    set -euo pipefail
    if ! curl -fsS --max-time 3 "{{ api }}/healthz" > /dev/null 2>&1; then
        echo "❌ Nothing answering at {{ api }}" >&2
        echo "   Start a local-mode worker (it serves the API too):" >&2
        echo "     just app/worker/local" >&2
        exit 1
    fi
    cd app/test
    QUEUE_ID={{ queue }} API_URL={{ api }} deno test \
        --unsafely-ignore-certificate-errors --allow-all \
        --unstable-broadcast-channel --unstable-cron --unstable-kv \
        src/skill_compute_queues_test.ts
    echo "✅ compute-queues skill API tests"

# Spawns real `claude -p` sessions: costs tokens, not deterministic, needs
# `claude` on PATH and a local-mode worker first: just app/worker/local

# Test that an LLM with the compute-queues skill really builds a working container
test-skill-ai +args="":
    #!/usr/bin/env bash
    set -euo pipefail
    deno run --allow-all app/test/skill-ai/run.ts {{ args }}

# Test MCP server (requires local dev stack to be running)
test-mcp:
    #!/usr/bin/env bash
    set -e

    # Try to find the MCP health endpoint
    echo "Checking if local dev stack is running..."

    # Try localhost:8000 (common local worker port)
    MCP_HEALTH_URL=""
    if curl -f -s http://localhost:8000/mcp/health > /dev/null 2>&1; then
        MCP_HEALTH_URL="http://localhost:8000"
        echo "✅ MCP server detected at http://localhost:8000"
    # Try localhost on other common ports
    elif curl -f -s http://localhost:443/mcp/health > /dev/null 2>&1; then
        MCP_HEALTH_URL="http://localhost:443"
        echo "✅ MCP server detected at http://localhost:443"
    else
        echo ""
        echo "❌ Error: MCP server health endpoint not responding!"
        echo ""
        echo "Tried:"
        echo "  - http://localhost:8000/mcp/health"
        echo "  - http://localhost:443/mcp/health"
        echo ""
        echo "Please start the local stack first:"
        echo "  just dev local"
        echo ""
        echo "Then verify it's running:"
        echo "  curl http://localhost:8000/mcp/health"
        echo ""
        exit 1
    fi

    echo ""

    # Set environment for local mode
    # Tests run on host machine, so use localhost
    export QUEUE_ID="local"
    export API_URL="http://localhost:8000"

    echo "Running MCP tests..."
    echo "  QUEUE_ID: $QUEUE_ID"
    echo "  API_URL: $API_URL"
    echo "  Health check: $MCP_HEALTH_URL/mcp/health"
    echo ""

    # Run the MCP tests with --no-check to bypass AWS SDK type errors
    cd app/test
    deno test --allow-all --no-check --unstable-broadcast-channel --unstable-kv --unstable-cron src/mcp_*.ts

    echo ""
    echo "✅ MCP tests completed successfully!"

# Watch the local dev stack, running the tests when files change
@watch mode="remote" +args="":
    just app watch {{ mode }} {{ args }}

# Bump the version, commit, CI will deploy and publish artifacts
@deploy version="":
    just app/deploy {{ version }}

# Shut Down Development Environment
@down mode="remote" +args="": (_validate_mode mode)
    just app/down {{ mode }} {{ args }}

# Clean Up Project
@clean mode="remote" +args="": (_validate_mode mode)
    just app/clean {{ mode }} {{ args }}

# Run Linting
@lint:
    just app/lint

# Run Lint-Fix Commands
@lint-fix:
    just app/lint-fix

# Run Fix Commands
@fix:
    just app/fix

# Publish Versioned Artifacts

# Usage: just publish-versioned-artifacts [version]
@publish-versioned-artifacts version="":
    just app/publish-versioned-artifacts {{ version }}

# Run Local Workers

# Usage: just run-local-workers
run-local-workers: publish-versioned-artifacts
    #!/usr/bin/env bash
    # Replace this with your image name (without tag)
    IMAGE_NAME="metapage/metaframe-docker-worker"

    # Get all container IDs for a given image name, ignoring the tag part
    CONTAINER_IDS=$(docker ps -a --format "{{{{.ID}}"  | xargs docker inspect --format '{{{{.Id}} {{{{.Config.Image}}' | grep $IMAGE_NAME | cut -d ' ' -f 1)

    if [ -z "$CONTAINER_IDS" ]; then
      echo "No containers found for image: $IMAGE_NAME"
    else
      echo "Found containers for image: $IMAGE_NAME"
      # Stop and remove the containers
      for CONTAINER_ID in $CONTAINER_IDS; do
        echo "Stopping container $CONTAINER_ID"
        docker stop $CONTAINER_ID
        echo "Removing container $CONTAINER_ID"
        docker rm $CONTAINER_ID
      done
      echo "All containers removed."
    fi

    VERSION=$(cat app/worker/mod.json | jq -r .version)
    docker run --restart unless-stopped -tid -v /var/run/docker.sock:/var/run/docker.sock -v /tmp:/tmp metapage/metaframe-docker-worker:$VERSION run --cpus=2 public1
    docker run --restart unless-stopped -tid -v /var/run/docker.sock:/var/run/docker.sock -v /tmp:/tmp metapage/metaframe-docker-worker:$VERSION run --cpus=2 ${DIONS_SECRET_QUEUE}

# Quick compilation checks
@check:
    just app check

###############################################################
# pr-daemon (https://github.com/metapages/pr-daemon)
#
# One preview stack per branch, with nothing shared between them.
#
# The stack is a single local-mode worker: it is the API, the queue and the
# static file server for the built browser app, all on one port. That is why
# these recipes don't use docker compose like `just dev local` does — the
# compose stack needs traefik, mkcert certs and /etc/hosts entries for
# worker-metaframe.localhost, and it hard-codes one host port. A plain
# `http://localhost:<port>` is what pr-daemon wants, and it works behind its
# public-preview proxy without a certificate. Everything the browser calls is
# on that one origin, so there are no extra PR_DAEMON_ROUTE services.
#
# Everything a branch owns is derived from the branch name: the port, the data
# directory, the worker id (which labels its job containers). Two branches run
# at once without touching each other. The machine-wide docker network and
# image cache volume are deliberately left alone by `down`.
###############################################################

PR_DAEMON_STATE_ROOT := "/tmp/pr-daemon-compute-grid"

# Print the branch's slug, port, state dir and worker id, as shell assignments
_pr-daemon-env:
    #!/usr/bin/env bash
    set -euo pipefail
    branch="$(git rev-parse --abbrev-ref HEAD)"
    if [ "$branch" = "HEAD" ]; then
        branch="detached-$(git rev-parse --short HEAD)"
    fi
    # docker labels and file names are happier without the rest of git's alphabet
    slug="$(printf '%s' "$branch" \
        | tr '[:upper:]' '[:lower:]' \
        | sed -E 's/[^a-z0-9_-]+/-/g' \
        | cut -c1-40 \
        | sed -E 's/-+$//')"
    [ -n "$slug" ] || slug="branch"
    if command -v shasum > /dev/null 2>&1; then
        digest="$(printf '%s' "$branch" | shasum -a 256 | cut -c1-8)"
    else
        digest="$(printf '%s' "$branch" | sha256sum | cut -c1-8)"
    fi
    # 20000-39999: above the ports this repo's dev and test stacks use
    printf 'branch=%q\n' "$branch"
    printf 'slug=%q\n' "$slug"
    printf 'state=%q\n' "{{ PR_DAEMON_STATE_ROOT }}/$slug"
    printf 'worker_id=%q\n' "prd-$slug"
    printf 'base_port=%q\n' "$(( 20000 + (0x$digest % 20000) ))"

# Start this branch's preview stack and print its URL (used by pr-daemon)
pr-daemon-up:
    #!/usr/bin/env bash
    set -euo pipefail

    # fd 3 is the real stdout and carries exactly one line: the URL. Everything
    # else goes to stderr, because pr-daemon reads the last URL-only line of
    # stdout and nothing else.
    exec 3>&1
    exec 1>&2

    eval "$(just _pr-daemon-env)"
    pidfile="$state/worker.pid"
    portfile="$state/port"
    log="$state/worker.log"
    data="$state/worker-data"
    mkdir -p "$state"

    answers() { curl -fsS --max-time 5 "http://localhost:$1/healthz" > /dev/null 2>&1; }

    # Idempotent: if this branch's stack is already answering, say so and stop.
    if [ -f "$portfile" ]; then
        port="$(cat "$portfile")"
        if answers "$port"; then
            echo "✅ preview for '$branch' is already up on port $port"
            echo "http://localhost:$port" >&3
            exit 0
        fi
        echo "stale stack for '$branch' on port $port — restarting it"
    fi

    # Kill anything left from a previous run of this branch before rebinding.
    just pr-daemon-down > /dev/null 2>&1 || true
    mkdir -p "$state"

    port="$base_port"
    while lsof -nP -iTCP:"$port" -sTCP:LISTEN > /dev/null 2>&1; do
        port=$(( port + 1 ))
        if [ "$port" -ge 40000 ]; then
            echo "💥 no free port in 20000-39999" >&2
            exit 1
        fi
    done

    echo "branch     $branch"
    echo "port       $port"
    echo "data       $data"
    echo "worker id  $worker_id"
    # The origin the browser reaches this stack at. With a preview domain that
    # is the public URL, and nothing else: browsers refuse to let a public page
    # call loopback, so no PR_DAEMON_ROUTE lines are printed — the API, the
    # websockets, job inputs/outputs and the UI are all this one origin.
    origin="${PR_DAEMON_PUBLIC_URL:-http://localhost:$port}"
    origin="${origin%/}"
    echo "origin     $origin"

    # The local-mode worker serves ../browser/dist, so the bundle has to exist
    # before it starts. This is the slow step on a cold worktree: npm i, tsc,
    # then vite build.
    echo "--- building the browser bundle ---"
    just app/shared/build
    # SERVER_ORIGIN empty: the app takes its API origin from location.origin
    # (app/browser/src/config.tsx), so a stray value from a .env can't bake a
    # local address into the bundle. The "local" queue would otherwise point
    # the browser at http://localhost:8000; here it is this stack.
    SERVER_ORIGIN= VITE_LOCAL_MODE_ORIGIN="$origin" just app/browser/build

    echo "--- starting the local-mode worker ---"
    mkdir -p "$data"
    cd app/worker
    # 3>&- matters: without it the worker inherits pr-daemon's stdout pipe and
    # pr-daemon waits forever for it to close. </dev/null because there is no TTY.
    nohup deno run --allow-all --unstable-kv --unstable-cron src/cli.ts run \
        --mode=local \
        --cpus=2 \
        --gpus=0 \
        --port="$port" \
        --data-directory="$data" \
        --id="$worker_id" \
        > "$log" 2>&1 3>&- < /dev/null &
    worker_pid=$!
    disown "$worker_pid" 2>/dev/null || true
    echo "$worker_pid" > "$pidfile"
    echo "$port" > "$portfile"

    # Return only once the app answers. Docker image pulls on a cold machine
    # are what makes this slow; pr-daemon allows 30 minutes.
    for _ in $(seq 1 300); do
        if answers "$port"; then
            # /healthz is the worker. Also prove the UI is actually being served,
            # so a missing dist directory fails here instead of in a browser.
            if ! curl -fsS --max-time 10 "http://localhost:$port/" | grep -qi '<html'; then
                echo "💥 worker is up but is not serving app/browser/dist" >&2
                tail -40 "$log" >&2 || true
                exit 1
            fi
            echo "✅ preview for '$branch' up on http://localhost:$port"
            echo "http://localhost:$port" >&3
            exit 0
        fi
        if ! kill -0 "$worker_pid" 2> /dev/null; then
            echo "💥 the worker exited before it answered:" >&2
            tail -60 "$log" >&2 || true
            exit 1
        fi
        sleep 1
    done

    echo "💥 the worker never answered on port $port:" >&2
    tail -60 "$log" >&2 || true
    exit 1

# Stop this branch's preview stack and delete its data (used by pr-daemon)
pr-daemon-down:
    #!/usr/bin/env bash
    # No `set -e`: this has to finish, and succeed, when nothing is running.
    set -uo pipefail

    eval "$(just _pr-daemon-env)"
    pidfile="$state/worker.pid"

    stop() {
        local pid="$1"
        kill -0 "$pid" 2> /dev/null || return 0
        kill -TERM "$pid" 2> /dev/null || true
        for _ in $(seq 1 15); do
            kill -0 "$pid" 2> /dev/null || return 0
            sleep 1
        done
        kill -KILL "$pid" 2> /dev/null || true
    }

    if [ -f "$pidfile" ]; then
        stop "$(cat "$pidfile")"
    fi
    # Belt and braces: the pid file can be missing or wrong (a reboot, a kill -9
    # of the shell that started it). The --id is unique to this branch.
    for pid in $(pgrep -f -- "--id=$worker_id" 2> /dev/null); do
        stop "$pid"
    done

    # This branch's job containers, and nothing else. The shared network
    # (metaframe-container-worker-network) and the shared image cache volume
    # (metaframe-container-worker-cache) are machine-wide and stay.
    containers="$(docker ps -aq --filter "label=container.mtfm.io/worker=$worker_id" 2> /dev/null)"
    if [ -n "$containers" ]; then
        echo "removing this branch's job containers" >&2
        docker rm -f $containers > /dev/null 2>&1 || true
    fi

    rm -rf "$state"
    echo "✅ preview for '$branch' is down and its data is deleted" >&2

# Everything that must be green before a PR opens (used by pr-daemon)
pr-daemon-check:
    #!/usr/bin/env bash
    set -euo pipefail
    just check
    just fmt-check
    just lint
    just app/worker/test
    just app/shared/test
    just app/cli/test
    echo "✅ pr-daemon check passed"

# Format all supported files
@fmt +args="":
    deno fmt {{ args }} 
    find app/*/justfile -exec just --fmt --unstable -f {} {{ args }} \;
    just --fmt --unstable -f docs/justfile {{ args }}
    just app/browser/fmt
    just docs/fmt {{ args }}

# Format all supported files
@fmt-check +args="":
    deno fmt --check {{ args }} 
    find app/*/justfile -exec just --fmt --check --unstable -f {} {{ args }} \;
    just --fmt --check --unstable -f docs/justfile {{ args }}
    just app/browser/fmt-check
    just docs/fmt-check {{ args }}

# Run CI
@ci: fmt-check lint

# app subdirectory commands

alias app := _app

@_app +args="":
    just app/{{ args }}

# app subdirectory commands

alias worker := _worker

@_worker +args="":
    just app/worker/{{ args }}

# app subdirectory commands

alias browser := _browser

@_browser +args="":
    just app/browser/{{ args }}

# app subdirectory commands

alias api := _api

@_api +args="":
    just app/api/{{ args }}

# app subdirectory commands

alias shared := _shared

@_shared +args="":
    just app/shared/{{ args }}

# cloudflare subdirectory commands (the API on Cloudflare Workers, issue #308)

alias cloudflare := _cloudflare

@_cloudflare +args="":
    just app/cloudflare/{{ args }}

# docs subdirectory commands (VitePress site served at /docs)

alias docs := _docs

@_docs +args="":
    just docs/{{ args }}

@logs mode service:
    just app/logs {{ mode }} {{ service }}
