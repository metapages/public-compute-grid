import type { ContainerInspectInfo } from "dockerode";
import { basename } from "std/path";

import { docker } from "/@/queue/dockerClient.ts";

const readText = (path: string): string => {
  try {
    return Deno.readTextFileSync(path);
  } catch {
    return "";
  }
};

/**
 * Candidate ids for the container this process runs in, best first. The
 * mounts docker makes for /etc/hostname etc. name the container on cgroup v1
 * and v2 alike; /proc/self/cgroup only on v1; the hostname is the short id
 * unless `--hostname` was given.
 */
export const ownContainerIdCandidates = (
  mountinfo = readText("/proc/self/mountinfo"),
  cgroup = readText("/proc/self/cgroup"),
  hostname = (() => {
    try {
      return Deno.hostname();
    } catch {
      return "";
    }
  })(),
): string[] => {
  const ids: string[] = [];
  for (const match of mountinfo.matchAll(/\/containers\/([0-9a-f]{64})\//g)) {
    ids.push(match[1]);
  }
  for (const match of cgroup.matchAll(/(?:docker[-/])([0-9a-f]{64})/g)) {
    ids.push(match[1]);
  }
  if (hostname) {
    ids.push(hostname);
  }
  return [...new Set(ids)];
};

export const isInContainer = (): boolean => {
  try {
    Deno.statSync("/.dockerenv");
    return true;
  } catch {
    return ownContainerIdCandidates(undefined, undefined, "").length > 0;
  }
};

/**
 * Inspect the container this worker runs in, through the docker socket. Null
 * when not in a container, or when the reachable docker daemon is not the one
 * running us (the standalone image talks to its own dockerd).
 */
export const inspectOwnContainer = async (): Promise<ContainerInspectInfo | null> => {
  for (const id of ownContainerIdCandidates()) {
    try {
      const info = await docker.getContainer(id).inspect();
      if (info?.Id?.startsWith(id) || info?.Config?.Hostname === id) {
        return info;
      }
    } catch {
      // not this one
    }
  }
  return null;
};

/** A `deno compile`d worker binary, as opposed to `deno run src/cli.ts`. */
export const isCompiledBinary = (): boolean => {
  const exe = basename(Deno.execPath());
  return exe !== "deno" && exe !== "deno.exe";
};

/**
 * Replace this process with `path` (execv): same pid, same environment, so a
 * supervisor (systemd, a shell, tmux) sees one process throughout. Only
 * returns if the exec failed.
 */
export const execInPlace = (path: string, args: string[]): Error => {
  const libcPath = Deno.build.os === "darwin" ? "/usr/lib/libSystem.B.dylib" : "libc.so.6";
  try {
    const libc = Deno.dlopen(libcPath, {
      execv: { parameters: ["buffer", "buffer"], result: "i32" },
    });
    const encoder = new TextEncoder();
    const cString = (s: string) => encoder.encode(`${s}\0`);
    const argv = [path, ...args].map(cString);
    const pointers = new BigUint64Array(argv.length + 1);
    argv.forEach((buffer, i) => {
      pointers[i] = BigInt(Deno.UnsafePointer.value(Deno.UnsafePointer.of(buffer)));
    });
    const result = libc.symbols.execv(cString(path), new Uint8Array(pointers.buffer));
    libc.close();
    return new Error(`execv ${path} failed (${result})`);
  } catch (err) {
    return err instanceof Error ? err : new Error(`${err}`);
  }
};
