/**
 * The few `Deno` globals that dependencies read while their modules load
 * (std@0.224's fs reads Deno.build.os at import time). Nothing here makes Deno
 * APIs work on Workers: code that needs them at call time must not run here.
 * Shrink this as shared/ drops Deno-only imports; delete it when empty.
 */
class NotFound extends Error {}
class AlreadyExists extends Error {}

const g = globalThis as Record<string, unknown>;
g.Deno ??= {
  build: { os: "linux" },
  env: {
    get: (key: string) => (globalThis as { process?: { env?: Record<string, string> } }).process?.env?.[key],
    set: () => {},
    toObject: () => ({}),
  },
  errors: { NotFound, AlreadyExists },
};
