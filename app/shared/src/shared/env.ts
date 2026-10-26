/**
 * Read an environment variable in any runtime we deploy on: Deno (Deno.env),
 * Node and Cloudflare Workers (process.env — Workers populate it from vars and
 * secrets under nodejs_compat). Use this instead of Deno.env.get in code that
 * must run outside Deno, and call it lazily (not at module top level), since
 * Workers only populate the environment once the module graph is loaded.
 */
export const getEnv = (name: string): string | undefined => {
  const deno = (globalThis as { Deno?: { env?: { get(key: string): string | undefined } } }).Deno;
  if (deno?.env) {
    try {
      return deno.env.get(name);
    } catch {
      // no --allow-env: fall through
    }
  }
  const proc = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process;
  return proc?.env?.[name];
};
