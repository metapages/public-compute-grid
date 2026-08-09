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
    echo -e "       api deployment config: {{ green }}https://dash.deno.com/projects/compute-queue-api{{ normal }}"

# Validate mode
@_validate_mode mode="":
    @if [ "{{ mode }}" = "remote" ] || [ "{{ mode }}" = "local" ]; then :; else echo "Error: Mode must be 'remote' or 'local'" >&2; exit 1; fi

# Start Development Environment
@dev mode="remote" +args="": (_validate_mode mode)
    just app/dev {{ mode }} {{ args }}

# Runs All Functional Tests and checks code (for the mode specified)
@test mode="remote":
    just app test {{ mode }}

# Runs All Functional Tests and checks code
test-all:
    just app test-all

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

# docs subdirectory commands (VitePress site served at /docs)

alias docs := _docs

@_docs +args="":
    just docs/{{ args }}

@logs mode service:
    just app/logs {{ mode }} {{ service }}
