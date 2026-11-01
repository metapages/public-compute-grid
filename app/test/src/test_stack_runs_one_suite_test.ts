import { assert, assertEquals } from "std/assert";

/**
 * `just test` starts the test service and then `exec`s the one suite run, so the
 * service itself must idle: `just dev`'s `deno test --watch` is only its default.
 * A stack whose command ignores TEST_SERVICE_COMMAND runs two suites against one
 * queue. They replace each other's jobs in shared namespaces and cancel and queue
 * behind each other, which fails one random job test per run and looks like
 * saturation (#314).
 */
Deno.test("every test stack lets `just test` replace its watch run", async () => {
  const root = new URL("../../../", import.meta.url);
  const files: string[] = [];
  for await (const { name } of Deno.readDir(root)) {
    if (/^docker-compose-test-.*\.yml$/.test(name)) {
      files.push(name);
    }
  }
  assertEquals(files.sort(), [
    "docker-compose-test-cloudflare.yml",
    "docker-compose-test-local.yml",
    "docker-compose-test-remote.yml",
  ]);

  for (const file of files) {
    const compose = await Deno.readTextFile(new URL(file, root));
    assert(
      /^\s*command: \$\{TEST_SERVICE_COMMAND:-/m.test(compose),
      `${file}: the test service's command must be overridable by TEST_SERVICE_COMMAND`,
    );
  }
});
