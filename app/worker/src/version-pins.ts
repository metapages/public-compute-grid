/**
 * Keeps every copy of the worker version in the repo in step with
 * app/worker/mod.json, the single source of truth (issue #336).
 *
 *   deno run --allow-read --allow-write --allow-run src/version-pins.ts sync
 *   deno run --allow-read --allow-run src/version-pins.ts check
 *
 * A pin is `metaframe-docker-worker:<semver>` (any `-standalone` style suffix
 * is kept), plus the `Version:` lines of llms.txt. Tags that aren't a semver
 * (`:latest`, `:local`, `:$VERSION`) are not pins and are left alone.
 *
 * A line that intentionally pins another version opts out with the marker
 * `worker-version-pin: ignore`, on that line or the line above it.
 */
import mod from "../mod.json" with { type: "json" };

export const IGNORE_MARKER = "worker-version-pin: ignore";

const SEMVER = String.raw`\d+\.\d+\.\d+`;

interface Pattern {
  // Group 1 is the text before the version, group 2 the version itself
  regex: RegExp;
  appliesTo: (path: string) => boolean;
}

const PATTERNS: Pattern[] = [
  {
    // The suffix (-standalone) follows the version and is not captured, so it is kept
    regex: new RegExp(String.raw`(metaframe-docker-worker:)(${SEMVER})(?![\d.])`, "g"),
    appliesTo: () => true,
  },
  {
    // "> Version: 0.54.87" and "- **Version**: Current 0.54.87"
    regex: new RegExp(String.raw`(Version\**:\s*(?:Current\s+)?)(${SEMVER})(?![\d.])`, "g"),
    appliesTo: (path) => basename(path) === "llms.txt",
  },
];

export interface Pin {
  path: string;
  // 1-indexed
  line: number;
  found: string;
}

const basename = (path: string) => path.split("/").pop() ?? path;

/**
 * Lockfiles and changelogs record history, and test fixtures use whatever
 * versions the test needs, so none of them are ever rewritten
 */
export const isExcludedPath = (path: string): boolean => {
  const name = basename(path).toLowerCase();
  return name.endsWith(".lock") || name.includes("lock.json") || name.includes("lock.yaml") ||
    name.startsWith("changelog") || /[._]test\.[jt]sx?$/.test(name);
};

/**
 * Rewrites every pin in `text` to `version`. Returns the new text and the pins
 * that were different before the rewrite.
 */
export const syncPins = (
  path: string,
  text: string,
  version: string,
): { text: string; stale: Pin[] } => {
  const stale: Pin[] = [];
  if (isExcludedPath(path)) {
    return { text, stale };
  }
  const patterns = PATTERNS.filter((p) => p.appliesTo(path));
  const lines = text.split("\n");
  const out = lines.map((line, i) => {
    if (line.includes(IGNORE_MARKER) || (i > 0 && lines[i - 1].includes(IGNORE_MARKER))) {
      return line;
    }
    for (const { regex } of patterns) {
      line = line.replace(regex, (match, prefix: string, found: string) => {
        if (found === version) {
          return match;
        }
        stale.push({ path, line: i + 1, found });
        return `${prefix}${version}`;
      });
    }
    return line;
  });
  return { text: out.join("\n"), stale };
};

/** Files tracked by git, relative to the repo root */
const trackedFiles = async (root: string): Promise<string[]> => {
  const { stdout } = await new Deno.Command("git", { args: ["ls-files", "-z"], cwd: root, stdout: "piped" })
    .output();
  return new TextDecoder().decode(stdout).split("\0").filter(Boolean);
};

const repoRoot = async (): Promise<string> => {
  const { stdout } = await new Deno.Command("git", {
    args: ["rev-parse", "--show-toplevel"],
    cwd: new URL(".", import.meta.url).pathname,
    stdout: "piped",
  }).output();
  return new TextDecoder().decode(stdout).trim();
};

/**
 * Finds (check) or rewrites (sync) every stale pin in the repo.
 * Returns the pins that did not match `version`.
 */
export const processRepo = async (
  root: string,
  version: string,
  write: boolean,
): Promise<Pin[]> => {
  const stale: Pin[] = [];
  for (const path of await trackedFiles(root)) {
    if (isExcludedPath(path)) {
      continue;
    }
    let text: string;
    try {
      const bytes = await Deno.readFile(`${root}/${path}`);
      // Binary files (images, fonts) have no pins
      if (bytes.includes(0)) {
        continue;
      }
      text = new TextDecoder().decode(bytes);
    } catch (_) {
      // Tracked but deleted in the working tree
      continue;
    }
    const result = syncPins(path, text, version);
    if (result.stale.length > 0) {
      stale.push(...result.stale);
      if (write) {
        await Deno.writeTextFile(`${root}/${path}`, result.text);
      }
    }
  }
  return stale;
};

if (import.meta.main) {
  const command = Deno.args[0];
  if (command !== "sync" && command !== "check") {
    console.error("usage: version-pins.ts sync|check");
    Deno.exit(2);
  }
  const version = mod.version;
  const root = await repoRoot();
  const stale = await processRepo(root, version, command === "sync");
  if (command === "sync") {
    for (const pin of stale) {
      console.log(`${pin.path}:${pin.line}: ${pin.found} -> ${version}`);
    }
    console.log(`✅ ${stale.length} worker version pin(s) updated to ${version}`);
  } else if (stale.length > 0) {
    for (const pin of stale) {
      console.error(`${pin.path}:${pin.line}: ${pin.found} != ${version}`);
    }
    console.error(
      `💥 ${stale.length} worker version pin(s) differ from app/worker/mod.json (${version}).` +
        ` Fix with: just sync-worker-version (or mark an intentional pin with "${IGNORE_MARKER}")`,
    );
    Deno.exit(1);
  } else {
    console.log(`✅ every worker version pin matches app/worker/mod.json (${version})`);
  }
}
