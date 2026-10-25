/**
 * Create the bucket the local stacks use.
 *
 * The dev and test stacks start an empty S3 server, so something has to make
 * the bucket; nothing did, and the only reason the suite passed was that the
 * self-hosted CI runners kept the S3 data directory between runs. Run as a
 * one-shot step by the compose stacks, before the api and workers start.
 *
 * Refuses to touch a hosted endpoint: a local S3 is the only thing that should
 * ever have its bucket created from here.
 */
import { blobStoreConfigFromEnv, S3BlobStore } from "@shared/s3.ts";

const config = blobStoreConfigFromEnv();

if (!config.endpoint) {
  console.error("No AWS_ENDPOINT set: refusing to create a bucket on AWS. Nothing to do.");
  Deno.exit(1);
}
if (/r2\.cloudflarestorage\.com|amazonaws\.com/.test(config.endpoint)) {
  console.error(`AWS_ENDPOINT is a hosted endpoint (${config.endpoint}): refusing to create a bucket there.`);
  Deno.exit(1);
}

await new S3BlobStore(config).ensureBucket();
console.log(`\u2705 bucket ${config.bucket} exists at ${config.endpoint}`);
