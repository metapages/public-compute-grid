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

FILES="SKILL.md references/rest-api.md references/backend-patterns.md"

echo "Installing the compute-queues skill"
echo "  from: $SRC"
echo "  into: $TARGET"

mkdir -p "$TARGET/references"

for f in $FILES; do
  if ! curl -fsSL "$SRC/$f" -o "$TARGET/$f"; then
    echo "failed to download $SRC/$f" >&2
    exit 1
  fi
  echo "  ✓ $f"
done

cat <<EOF

Installed. Restart your agent so it picks up the new skill.

Try: "run 'echo hello' in an alpine container on a compute queue"

Docs: $BASE/docs/
EOF
