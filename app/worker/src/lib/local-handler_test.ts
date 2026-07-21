import { assertEquals } from "std/assert";
import { join } from "std/path";

import { saveContentAddressedBlob } from "/@/lib/local-handler.ts";

const streamOf = (bytes: Uint8Array): ReadableStream<Uint8Array> =>
  new ReadableStream({
    start(controller) {
      controller.enqueue(bytes);
      controller.close();
    },
  });

const bigBytes = (seed: string): Uint8Array => {
  let s = seed;
  while (s.length < 200_000) s += s;
  return new TextEncoder().encode(s);
};

Deno.test("saveContentAddressedBlob: writes a new blob atomically", async () => {
  const dir = await Deno.makeTempDir();
  try {
    const bytes = bigBytes("hello-");
    const wrote = await saveContentAddressedBlob(dir, "sha-a", streamOf(bytes));
    assertEquals(wrote, true);
    assertEquals(await Deno.readFile(join(dir, "sha-a")), bytes);
    // no temp files left behind
    const leftovers = [];
    for await (const e of Deno.readDir(dir)) {
      if (e.name.includes(".tmp.")) leftovers.push(e.name);
    }
    assertEquals(leftovers, []);
  } finally {
    await Deno.remove(dir, { recursive: true });
  }
});

Deno.test("saveContentAddressedBlob: skips an existing non-empty blob without touching the inode", async () => {
  const dir = await Deno.makeTempDir();
  try {
    const bytes = bigBytes("world-");
    await saveContentAddressedBlob(dir, "sha-b", streamOf(bytes));
    const before = await Deno.stat(join(dir, "sha-b"));

    // A duplicate PUT of the same content-addressed key must not rewrite the file.
    const wrote = await saveContentAddressedBlob(dir, "sha-b", streamOf(bytes));
    assertEquals(wrote, false);

    const after = await Deno.stat(join(dir, "sha-b"));
    // Same inode, unchanged mtime => the existing blob was never truncated/rewritten.
    assertEquals(after.ino, before.ino);
    assertEquals(after.mtime?.getTime(), before.mtime?.getTime());
    assertEquals(await Deno.readFile(join(dir, "sha-b")), bytes);
  } finally {
    await Deno.remove(dir, { recursive: true });
  }
});

Deno.test("saveContentAddressedBlob: concurrent duplicate writes never expose a truncated read", async () => {
  const dir = await Deno.makeTempDir();
  try {
    const bytes = bigBytes("concurrent-");
    const path = join(dir, "sha-c");

    // Seed the blob, then hammer it with duplicate saves while continuously
    // reading. Before the fix (in-place truncate) a read could observe a
    // zero/partial file; with skip-if-exists + atomic rename it never can.
    await saveContentAddressedBlob(dir, "sha-c", streamOf(bytes));

    let empties = 0;
    let partials = 0;
    const reads: Promise<void>[] = [];
    const writes: Promise<unknown>[] = [];
    for (let i = 0; i < 200; i++) {
      writes.push(saveContentAddressedBlob(dir, "sha-c", streamOf(bytes)).catch(() => {}));
      reads.push((async () => {
        const got = await Deno.readFile(path);
        if (got.length === 0) empties++;
        else if (got.length !== bytes.length) partials++;
      })());
    }
    await Promise.all([...writes, ...reads]);

    assertEquals(empties, 0, "a concurrent read observed an empty blob");
    assertEquals(partials, 0, "a concurrent read observed a partial blob");
    assertEquals(await Deno.readFile(path), bytes);
  } finally {
    await Deno.remove(dir, { recursive: true });
  }
});
