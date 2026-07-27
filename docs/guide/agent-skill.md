# Agent Skill

An [Agent Skill](https://code.claude.com/docs/en/skills) that teaches a coding agent to drive a compute queue correctly
— the endpoints, the three result patterns, and the handful of behaviours that trip people up (content-hash
deduplication, "queued but no worker", the two separate success checks).

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

Or just download the three files by hand:

- [`SKILL.md`](https://container.mtfm.io/docs/skill/compute-queues/SKILL.md)
- [`references/rest-api.md`](https://container.mtfm.io/docs/skill/compute-queues/references/rest-api.md)
- [`references/backend-patterns.md`](https://container.mtfm.io/docs/skill/compute-queues/references/backend-patterns.md)

## What it covers

| File                             | Contents                                                                   |
| -------------------------------- | -------------------------------------------------------------------------- |
| `SKILL.md`                       | The model, submit + the three result patterns, adding compute, files.      |
| `references/rest-api.md`         | Every endpoint, the full job definition, result shapes, worker CLI flags.  |
| `references/backend-patterns.md` | A reusable client module, a complete Deno service, recipes, failure modes. |

The agent loads `SKILL.md` when a task looks relevant and pulls in a reference only when it needs the detail — so the
common case stays cheap.

## Try it

Once installed, prompts like these should just work:

- _"Run this Python script on a compute queue and give me the output"_
- _"Add a job queue to this Express app so uploads get processed in a container"_
- _"Set up a worker on this machine and point it at a private queue"_
- _"Why is my job stuck in Queued?"_

## Without installing anything

- `https://container.mtfm.io/llms.txt` — a single-file project summary for an LLM context window.
- These docs are plain markdown; point a tool at [`/guide/backend-integration`](/guide/backend-integration) and it has
  what it needs.

## For maintainers

The skill lives in the repo at `docs/public/skill/compute-queues/` and is published verbatim by the docs build
(VitePress copies `public/` unmodified). Editing those files and deploying is the whole release process — there is no
bundling step.

Keep it in sync with [Backend integration](/guide/backend-integration); they describe the same behaviour for two
different audiences.
