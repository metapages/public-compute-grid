#!/bin/sh
# Install the `compute-queues` Agent Skill.
#
#   curl -fsSL https://container.mtfm.io/docs/skill/install.sh | sh
#
# Optional:
#   SKILLS_DIR=~/.cursor/skills   installs somewhere other than ~/.claude/skills
#   COMPUTE_QUEUES_BASE=https://my-deployment.example.com   installs from your own stack
set -eu

BASE="${COMPUTE_QUEUES_BASE:-https://container.mtfm.io}"
SKILLS_DIR="${SKILLS_DIR:-$HOME/.claude/skills}"
TARGET="$SKILLS_DIR/compute-queues"
SRC="$BASE/docs/skill/compute-queues"

FILES="SKILL.md
references/build-and-iterate.md
references/job-definition.md
references/backend-patterns.md
references/rest-api.md
scripts/cq.mjs"

echo "Installing the compute-queues skill"
echo "  from: $SRC"
echo "  into: $TARGET"

for f in $FILES; do
  mkdir -p "$(dirname "$TARGET/$f")"
  if ! curl -fsSL "$SRC/$f" -o "$TARGET/$f"; then
    echo "failed to download $SRC/$f" >&2
    exit 1
  fi
  echo "  ✓ $f"
done

# The helper is executed directly by the agent.
chmod +x "$TARGET/scripts/cq.mjs"

cat <<EOF

Installed. Restart your agent so it picks up the new skill.

Try: "build me a container that runs this script and show me the output"
     "add a job queue to this app so uploads get processed in a container"

Docs: $BASE/docs/
EOF
