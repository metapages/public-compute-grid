import { assertEquals, assertExists } from "std/assert";

import { convertJobOutputDataRefsToExpectedFormat, dataUrlToDataRef } from "@shared/dataref.ts";
import { DataRefType } from "@shared/types.ts";

Deno.test("test something", () => {
  // console.log("test something", convertJobOutputDataRefsToExpectedFormat);
  assertExists(convertJobOutputDataRefsToExpectedFormat);
});

Deno.test("dataUrlToDataRef: text/x-uri becomes a url ref, not downloaded", () => {
  const url = "https://metapage.io/f/bc1082dc-e276-4caa-abd1-576de181c604";
  const dataUrl = `data:text/x-uri;charset=utf-8,${encodeURIComponent(url)}`;
  assertEquals(dataUrlToDataRef(dataUrl), {
    type: DataRefType.url,
    value: url,
  });
});

Deno.test("dataUrlToDataRef: text/x-uri with extra params", () => {
  const url = "https://container.mtfm.io/f/9f86d0";
  const dataUrl = `data:text/x-uri;type=Uint8Array;charset=utf-8,${encodeURIComponent(url)}`;
  assertEquals(dataUrlToDataRef(dataUrl), {
    type: DataRefType.url,
    value: url,
  });
});

Deno.test("dataUrlToDataRef: base64 payload", () => {
  assertEquals(
    dataUrlToDataRef("data:application/octet-stream;base64,aGVsbG8="),
    { type: DataRefType.base64, value: "aGVsbG8=" },
  );
});

Deno.test("dataUrlToDataRef: json payload", () => {
  const dataUrl = `data:application/json;charset=utf-8,${encodeURIComponent(JSON.stringify({ a: 1 }))}`;
  // json refs hold the parsed value, matching DataRefType.json elsewhere
  assertEquals(dataUrlToDataRef(dataUrl) as unknown, {
    type: DataRefType.json,
    value: { a: 1 },
  });
});

Deno.test("dataUrlToDataRef: malformed json falls back to utf8", () => {
  const dataUrl = "data:application/json;charset=utf-8,not-json";
  assertEquals(dataUrlToDataRef(dataUrl), {
    type: DataRefType.utf8,
    value: "not-json",
  });
});

Deno.test("dataUrlToDataRef: text payload", () => {
  const dataUrl = `data:text/plain;charset=utf-8,${encodeURIComponent("hello world")}`;
  assertEquals(dataUrlToDataRef(dataUrl), {
    type: DataRefType.utf8,
    value: "hello world",
  });
});

Deno.test("dataUrlToDataRef: empty payload", () => {
  assertEquals(dataUrlToDataRef("data:text/plain;charset=utf-8,"), {
    type: DataRefType.utf8,
    value: "",
  });
});

Deno.test("dataUrlToDataRef: non-datarefs return undefined", () => {
  assertEquals(dataUrlToDataRef("just a string"), undefined);
  assertEquals(dataUrlToDataRef("https://example.com"), undefined);
  assertEquals(dataUrlToDataRef("data:no-comma"), undefined);
  assertEquals(dataUrlToDataRef({ type: "url", value: "x" }), undefined);
  assertEquals(dataUrlToDataRef(undefined), undefined);
  assertEquals(dataUrlToDataRef(42), undefined);
});
