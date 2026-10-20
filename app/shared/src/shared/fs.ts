/**
 * The std/fs helpers shared/ uses, over node:fs, which Deno, Node and Workers
 * (nodejs_compat) all provide. std/fs reads Deno globals when it is imported,
 * so it cannot be in any module the Cloudflare Worker loads.
 */
import { closeSync, existsSync as nodeExistsSync, mkdirSync, openSync } from "node:fs";
import { mkdir, stat } from "node:fs/promises";
import { dirname } from "node:path";

export const ensureDir = async (path: string): Promise<void> => {
  await mkdir(path, { recursive: true });
};

export const ensureDirSync = (path: string): void => {
  mkdirSync(path, { recursive: true });
};

export const exists = async (path: string): Promise<boolean> => {
  try {
    await stat(path);
    return true;
  } catch {
    return false;
  }
};

export const existsSync = (path: string): boolean => nodeExistsSync(path);

/** Create the file, and its directory, if missing */
export const ensureFileSync = (path: string): void => {
  mkdirSync(dirname(path), { recursive: true });
  if (!nodeExistsSync(path)) {
    closeSync(openSync(path, "a"));
  }
};
