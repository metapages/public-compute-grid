import { assertEquals, assertNotEquals } from "std/assert";

import { copyLargeBlobsToCloud, dataRefToBuffer, dataUrlToDataRef } from "@shared/dataref.ts";
import { DataRefType, type InputsRefs } from "@shared/types.ts";

/**
 * End to end check of the bug this fixes: metapage.io hands a container
 * metaframe a v2 dataref string, and the container must end up with the FILE
 * CONTENTS, not the literal "data:text/x-uri;..." text.
 *
 * The server here stands in for metapage.io/f/:fileId, which resolves the file
 * id server side and 302s to a presigned storage URL — so the redirect is part
 * of what has to work.
 */

const FILE_ID = "bc1082dc-e276-4caa-abd1-576de181c604";
const FILE_CONTENTS = "col_a,col_b\n1,2\n3,4\n";

type Fixture = {
  url: string;
  requested: string[];
  stop: () => Promise<void>;
};

const startFileServer = (): Fixture => {
  const requested: string[] = [];
  const server = Deno.serve(
    { port: 0, onListen: () => {} },
    (request: Request) => {
      const { pathname } = new URL(request.url);
      requested.push(pathname);
      // /f/:fileId redirects to storage, exactly like metapage.io does
      if (pathname === `/f/${FILE_ID}`) {
        return new Response(null, {
          status: 302,
          headers: { location: `/storage/${FILE_ID}?signature=abc` },
        });
      }
      if (pathname === `/storage/${FILE_ID}`) {
        return new Response(FILE_CONTENTS, {
          status: 200,
          headers: { "content-type": "text/csv" },
        });
      }
      return new Response("not found", { status: 404 });
    },
  );
  const port = (server.addr as Deno.NetAddr).port;
  return {
    url: `http://localhost:${port}/f/${FILE_ID}`,
    requested,
    stop: async () => {
      await server.shutdown();
    },
  };
};

Deno.test("e2e: a metapage.io url dataref reaches the container as file contents", async () => {
  const fixture = startFileServer();
  try {
    // Exactly what metapage.io's fs metaframe emits (urlToDataUrl)
    const metaframeInput = `data:text/x-uri;charset=utf-8,${encodeURIComponent(fixture.url)}`;

    // What useDockerJobDefinition now does with that metaframe input
    const inputRef = dataUrlToDataRef(metaframeInput);
    assertEquals(inputRef, {
      type: DataRefType.url,
      value: fixture.url,
    });

    // What the worker then does to materialise the container's input file
    const buffer = await dataRefToBuffer(inputRef!);
    const contents = new TextDecoder().decode(buffer);

    assertEquals(contents, FILE_CONTENTS);
    // The regression itself: the container used to get this literal string
    assertNotEquals(contents, metaframeInput);

    // The redirect was followed, so nothing depends on a presigned URL that
    // the browser resolved earlier
    assertEquals(fixture.requested, [
      `/f/${FILE_ID}`,
      `/storage/${FILE_ID}`,
    ]);
  } finally {
    await fixture.stop();
  }
});

Deno.test("e2e: the pre-fix path is what produced the broken input", async () => {
  // Before dataUrlToDataRef existed, isDataRef (v1 objects only) did not match
  // the string, so useDockerJobDefinition fell through to its utf8 branch.
  // This pins down what that produced, so the difference above is not a
  // test that would pass either way.
  const url = "https://metapage.io/f/" + FILE_ID;
  const metaframeInput = `data:text/x-uri;charset=utf-8,${encodeURIComponent(url)}`;

  const brokenRef = { type: DataRefType.utf8, value: metaframeInput };
  const contents = new TextDecoder().decode(await dataRefToBuffer(brokenRef));

  // no fetch happened, the container just got the reference as text
  assertEquals(contents, metaframeInput);
  assertEquals(contents.startsWith("data:text/x-uri"), true);
});

Deno.test("e2e: the browser never downloads the blob, the worker does", async () => {
  const fixture = startFileServer();
  try {
    const metaframeInput = `data:text/x-uri;charset=utf-8,${encodeURIComponent(fixture.url)}`;
    const inputs: InputsRefs = {
      "data.csv": dataUrlToDataRef(metaframeInput)!,
    };

    // copyLargeBlobsToCloud runs in the browser before the job is submitted:
    // a url ref must pass straight through, never fetched and re-uploaded
    const result = await copyLargeBlobsToCloud(inputs, "http://unused.invalid");

    assertEquals(result, inputs);
    assertEquals(fixture.requested, []);
  } finally {
    await fixture.stop();
  }
});

Deno.test("e2e: a large utf8 input is still uploaded and referenced by hash", async () => {
  // Guards the other half of copyLargeBlobsToCloud: plain values over the
  // threshold still get uploaded, so the url branch above did not swallow them.
  const uploads: string[] = [];
  const server = Deno.serve(
    { port: 0, onListen: () => {} },
    async (request: Request) => {
      const { pathname } = new URL(request.url);
      if (request.method === "PUT") {
        uploads.push(pathname);
        await request.arrayBuffer();
        return new Response("ok", { status: 200 });
      }
      return new Response("not found", { status: 404 });
    },
  );
  const address = `http://localhost:${(server.addr as Deno.NetAddr).port}`;
  try {
    const big = "x".repeat(500);
    const result = await copyLargeBlobsToCloud(
      { "big.txt": { type: DataRefType.utf8, value: big } },
      address,
    );

    assertEquals(result!["big.txt"].type, DataRefType.url);
    assertEquals(uploads.length, 1);
    assertEquals(
      result!["big.txt"].value,
      `${address}/f/${result!["big.txt"].hash}`,
    );
  } finally {
    await server.shutdown();
  }
});
