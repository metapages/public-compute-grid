// One S3-API blob store (R2 in production, MinIO locally), configured in shared.
// Via the @shared/ alias because the deploy build flattens the workspace layout.
export { getBlobStore } from "@shared/s3.ts";
