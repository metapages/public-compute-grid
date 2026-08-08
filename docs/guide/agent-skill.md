# Agent Skill

An [Agent Skill](https://code.claude.com/docs/en/skills) that teaches a coding agent to drive a compute queue — both
halves of it: building a container that actually works, and wiring job submission into an app.

## Install

```sh
curl -fsSL https://container.mtfm.io/docs/skill/install.sh | sh
```

Installs into `~/.claude/skills/compute-queues`. Restart your agent afterwards — skill frontmatter is read at startup.

Other locations and other deployments:

```sh
# a different harness's skills directory
curl -fsSL https://container.mtfm.io/docs/skill/install.sh | SKILLS_DIR=~/.cursor/skills sh

# install from your own deployment
curl -fsSL https://my-api.example.com/docs/skill/install.sh \
  | COMPUTE_QUEUES_BASE=https://my-api.example.com sh
```

## What it covers

| File                              | Contents                                                                          |
| --------------------------------- | ----------------------------------------------------------------------------------- |
| `SKILL.md`                        | The model, the facts that prevent most mistakes, the `cq` helper, both entry paths. |
| `references/build-and-iterate.md` | Build recipes, failure signatures and what each means, the raw HTTP behind `cq`.   |
| `references/job-definition.md`    | Every definition field, what is and is not part of the job hash, result shapes.    |
| `references/backend-patterns.md`  | A reusable client module, a complete Deno service, the restart-safe pattern.       |
| `references/rest-api.md`          | Every endpoint, request/response shapes, worker CLI flags.                          |
| `scripts/cq.mjs`                  | A zero-dependency helper CLI (node ≥ 18 or deno).                                   |

The agent loads `SKILL.md` when a task looks relevant and pulls in a reference only when it needs the detail — so the
common case stays cheap.

The helper is what keeps the build loop reliable: the tricky parts (content-addressed uploads, SSE log following, the
job cache, exit-code vs `finishedReason`) live in tested code instead of being re-derived each time.

```sh
cq run --dockerfile ./Dockerfile --context-dir ./ctx \
       --command 'python /app/main.py' \
       --input data.csv=@./data.csv \
       --output-dir ./outputs
```

`cq run` streams build and run logs live and **exits with the container's own exit code**, so `if cq run ...; then` is
a real test of whether the container works.

## Try it

Once installed, prompts like these should just work:

- _"Build me a container that runs this repo's test suite"_
- _"Package this script with its dependencies and run it on my data"_
- _"This Dockerfile won't build — fix it"_
- _"Run this Python script on a compute queue and give me the output"_
- _"Add a job queue to this Express app so uploads get processed in a container"_
- _"Why is my job stuck in Queued?"_

## Without installing anything

- `https://container.mtfm.io/llms.txt` — a single-file project summary for an LLM context window.
- These docs are plain markdown; point a tool at [`/guide/backend-integration`](/guide/backend-integration) and it has
  what it needs.

## For maintainers

The skill lives in the repo at `docs/public/skill/compute-queues/` and is published verbatim by the docs build
(VitePress copies `public/` unmodified). Editing those files and deploying is the whole release process — there is no
bundling step.

Keep it in sync with [Backend integration](/guide/backend-integration) and [Job definition](/guide/job-definition);
they describe the same behaviour for different audiences.

Working on the skill: symlink it into your agent's skills directory so repo edits are live, instead of reinstalling
after every change. The install also points `cq` at a **local** stack, so an agent using the dev skill cannot submit
jobs to the shared production queue by accident.

```sh
just dev-install-skill                  # -> http://localhost:8000, queue "local"  (just app/worker/local)
just dev-install-skill stack            # -> the compose stack, queue "local1"      (just dev)
just dev-install-skill prod             # no redirect; use the production defaults
just dev-install-skill worker ~/.cursor/skills   # a different skills directory
just dev-uninstall-skill                # unlinks and clears the redirect
```

The redirect is a gitignored `scripts/dev-target.json` written next to `cq.mjs`, so it exists only in a dev checkout —
installed users always get the production defaults. `cq` announces it on every run, and an explicit `--api` or
`CQ_API` still wins. `just docs/build` strips the file from `dist` so a manual deploy cannot publish it.

Restart the agent afterwards — frontmatter is read at startup, though edits to the body and to `cq.mjs` are picked up
without one.

Tests. A local-mode worker serves the API itself, so it is all the tests need — and it starts in seconds, unlike the
full compose stack:

```sh
just app/worker/local   # API on :8000, queue "local"
just test-skill         # deterministic: the endpoints the skill depends on
just test-skill-ai      # spawns real `claude -p` sessions and checks a container actually ran
```

`just test-skill-ai` is the only test that answers "does the skill text actually lead a model to a working container?".
Its prompts deliberately never name the skill — that would mask whether the skill gets selected at all. It costs tokens
and is not deterministic, so it is not part of `just test`.

### Why one skill and not two

An earlier split into `compute-queues` (integrating) and `container-builder` (building) failed a selection test: with
both installed and the skill unnamed, _"run this script in a container and show me the output"_ loaded
`compute-queues`, which hand-rolled a curl polling loop and never touched `cq` or the build/run log split — despite
that phrasing appearing almost verbatim in the other skill's description. The two competed on shared vocabulary
("compute queue", "run a container") and no description wording separates them, because the underlying request really
is both. One skill with progressive disclosure has no wrong branch to take.
