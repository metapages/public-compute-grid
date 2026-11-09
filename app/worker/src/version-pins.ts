/**
 * Keeps every copy of the worker version in the repo in step with
 * app/worker/mod.json, the single source of truth (issue #336).
 *
 *   deno run --allow-read --allow-write --allow-run src/version-pins.ts sync [--files-only] [root]
 *   deno run --allow-read --allow-run src/version-pins.ts check [root]
 *
 * `root` defaults to the repo this file is in. `sync --files-only` prints only
 * the paths it rewrote, one per line, on stdout (the per-pin log goes to
 * stderr), for `just deploy` to stage.
 *
 * A pin is `metaframe-docker-worker:<semver>` (any `-standalone` style suffix
 * is kept), plus the `Version:` lines of llms.txt. Tags that aren't a semver
 * (`:latest`, `:local`, `:$VERSION`) are not pins and are left alone.
 *
 * A line that intentionally pins another version opts out with the marker
 * `worker-version-pin: ignore`, either on that line or alone on the line above
 * it (a line above that is itself a pin only exempts itself).
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
  const hasPin = (line: string) => patterns.some(({ regex }) => new RegExp(regex.source).test(line));
  const lines = text.split("\n");
  const out = lines.map((line, i) => {
    const above = i > 0 ? lines[i - 1] : "";
    if (line.includes(IGNORE_MARKER) || (above.includes(IGNORE_MARKER) && !hasPin(above))) {
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

/**
 * Runs git and returns its stdout. Throws, with git's stderr, when git fails:
 * an empty file list from a failed `git ls-files` would otherwise pass the check
 */
const git = async (cwd: string, args: string[]): Promise<string> => {
  const { success, code, stdout, stderr } = await new Deno.Command("git", {
    args,
    cwd,
    stdout: "piped",
    stderr: "piped",
  }).output();
  if (!success) {
    const message = new TextDecoder().decode(stderr).trim();
    throw new Error(`git ${args.join(" ")} failed in ${cwd} (exit ${code}): ${message}`);
  }
  return new TextDecoder().decode(stdout);
};

/** Files tracked by git, relative to the repo root */
const trackedFiles = async (root: string): Promise<string[]> => {
  const files = (await git(root, ["ls-files", "-z"])).split("\0").filter(Boolean);
  if (files.length === 0) {
    throw new Error(`git ls-files found no tracked files in ${root}`);
  }
  return files;
};

export const repoRoot = async (cwd: string = new URL(".", import.meta.url).pathname): Promise<string> =>
  (await git(cwd, ["rev-parse", "--show-toplevel"])).trim();

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

const main = async (args: string[]): Promise<number> => {
  const filesOnly = args.includes("--files-only");
  const [command, rootArg, ...extra] = args.filter((a) => a !== "--files-only");
  if ((command !== "sync" && command !== "check") || extra.length > 0 || (filesOnly && command !== "sync")) {
    console.error("usage: version-pins.ts sync [--files-only] [root] | check [root]");
    return 2;
  }
  const version = mod.version;
  const root = await repoRoot(rootArg);
  const stale = await processRepo(root, version, command === "sync");
  if (command === "sync") {
    // With --files-only, stdout is just the rewritten paths
    const log = filesOnly ? console.error : console.log;
    for (const pin of stale) {
      log(`${pin.path}:${pin.line}: ${pin.found} -> ${version}`);
    }
    if (filesOnly) {
      for (const path of new Set(stale.map((pin) => pin.path))) {
        console.log(path);
      }
    }
    log(`✅ ${stale.length} worker version pin(s) updated to ${version}`);
  } else if (stale.length > 0) {
    for (const pin of stale) {
      console.error(`${pin.path}:${pin.line}: ${pin.found} != ${version}`);
    }
    console.error(
      `💥 ${stale.length} worker version pin(s) differ from app/worker/mod.json (${version}).` +
        ` Fix with: just sync-worker-version (or mark an intentional pin with "${IGNORE_MARKER}")`,
    );
    return 1;
  } else {
    console.log(`✅ every worker version pin matches app/worker/mod.json (${version})`);
  }
  return 0;
};

if (import.meta.main) {
  try {
    Deno.exit(await main(Deno.args));
  } catch (err) {
    console.error(`💥 ${err instanceof Error ? err.message : err}`);
    Deno.exit(1);
  }
}
