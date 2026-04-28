import { assertExists } from "std/assert";

import { convertJobOutputDataRefsToExpectedFormat } from "@shared/dataref.ts";

Deno.test("test something", () => {
  // console.log("test something", convertJobOutputDataRefsToExpectedFormat);
  assertExists(convertJobOutputDataRefsToExpectedFormat);
});
