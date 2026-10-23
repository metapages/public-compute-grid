/**
 * Write a D1 database id into wrangler.jsonc, for the binding with this
 * database_name. A regex, not a JSON round trip, so the comments survive.
 *
 *   deno run --allow-read --allow-write scripts/set-d1-id.ts <database_name> <database_id>
 */
const [name, id] = Deno.args;
if (!name || !/^[0-9a-f-]{36}$/.test(id ?? "")) {
  console.error("usage: set-d1-id.ts <database_name> <database_id>");
  Deno.exit(1);
}
const path = new URL("../wrangler.jsonc", import.meta.url);
const text = await Deno.readTextFile(path);
const pattern = new RegExp(`("database_name":\\s*"${name}",\\s*"database_id":\\s*")[^"]*(")`, "g");
const matches = text.match(pattern) ?? [];
if (matches.length === 0) {
  console.error(`No D1 binding named ${name} in wrangler.jsonc`);
  Deno.exit(1);
}
// More than one binding for the same database means the config has a duplicate
// — wrangler's "shall I add it for you?" prompt does that. Patching one of them
// at random leaves the other wrong, so refuse and let a human look.
if (matches.length > 1) {
  console.error(
    `${matches.length} D1 bindings named ${name} in wrangler.jsonc; remove the duplicate first`,
  );
  Deno.exit(1);
}
await Deno.writeTextFile(path, text.replace(pattern, `$1${id}$2`));
console.log(`✅ wrangler.jsonc: ${name} → ${id}`);
