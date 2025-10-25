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
    echo -e ""
    echo -e "    Current worker version: {{ cyan }}$(cat app/worker/mod.json | jq -r '.version'){{ normal }}"
    echo -e ""
    echo -e "    Quick links:"
    echo -e "       api local:             {{ green }}https://worker-metaframe.localhost:{{APP_PORT}}/{{ normal }}"
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
    just app/browser/fmt

# Format all supported files
@fmt-check +args="":
    deno fmt --check {{ args }} 
    find app/*/justfile -exec just --fmt --check --unstable -f {} {{ args }} \;
    just app/browser/fmt-check

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

@logs mode service:
    just app/logs {{mode}} {{service}}
