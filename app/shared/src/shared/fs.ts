/**
 * The std/fs helpers shared/ uses, without std/fs.
 *
 * Two constraints meet here. std/fs reads Deno globals when it is *imported*,
 * so it cannot appear anywhere in the Cloudflare Worker's module graph (and
 * jobtools.ts, which uses these, is in it). But the Deno runtimes — the API
 * server, the worker, the CLI, the tests — must keep behaving exactly as they
 * did: implementing these on node:fs alone changed how the worker materialized
 * job inputs in the docker stack, and the local suite failed because a served
 * file came back empty (bisected to this file).
 *
 * So: use Deno's own filesystem calls wherever Deno exists, and node:fs
 * otherwise (Workers, and the Node runtime the worker is headed for). Nothing
 * is read at import time, and the calls themselves only ever run where the
 * chosen implementation is the native one.
 */
import { closeSync, existsSync as nodeExistsSync, mkdirSync, openSync } from "node:fs";
import { mkdir, stat } from "node:fs/promises";
import { dirname } from "node:path";

type DenoFs = {
  mkdir(path: string, options?: { recursive?: boolean }): Promise<void>;
  mkdirSync(path: string, options?: { recursive?: boolean }): void;
  stat(path: string): Promise<unknown>;
  statSync(path: string): unknown;
  open(path: string, options: { create?: boolean; write?: boolean; read?: boolean }): Promise<{ close(): void }>;
};

/** Deno's own fs, when running on Deno. Looked up lazily, never at import. */
const denoFs = (): DenoFs | undefined => (globalThis as { Deno?: DenoFs }).Deno;

export const ensureDir = async (path: string): Promise<void> => {
  const deno = denoFs();
  if (deno) {
    await deno.mkdir(path, { recursive: true });
    return;
  }
  await mkdir(path, { recursive: true });
};

export const ensureDirSync = (path: string): void => {
  const deno = denoFs();
  if (deno) {
    deno.mkdirSync(path, { recursive: true });
    return;
  }
  mkdirSync(path, { recursive: true });
};

export const exists = async (path: string): Promise<boolean> => {
  const deno = denoFs();
  try {
    if (deno) {
      await deno.stat(path);
    } else {
      await stat(path);
    }
    return true;
  } catch {
    return false;
  }
};

export const existsSync = (path: string): boolean => {
  const deno = denoFs();
  if (!deno) {
    return nodeExistsSync(path);
  }
  try {
    deno.statSync(path);
    return true;
  } catch {
    return false;
  }
};

/** Create the file, and its directory, if missing */
export const ensureFileSync = (path: string): void => {
  ensureDirSync(dirname(path));
  if (existsSync(path)) {
    return;
  }
  const deno = denoFs();
  if (deno) {
    // Deno has no sync "create if missing" that leaves an existing file alone,
    // so open for append the same way the node branch does.
    const file = (deno as unknown as {
      openSync(path: string, options: { create: boolean; append: boolean }): { close(): void };
    }).openSync(path, { create: true, append: true });
    file.close();
    return;
  }
  closeSync(openSync(path, "a"));
};
