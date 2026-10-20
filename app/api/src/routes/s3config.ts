// One S3-API client (R2 in production, MinIO locally), configured in shared.
// Via the @shared/ alias because the deploy build flattens the workspace layout.
export { bucketParams, s3Client } from "@shared/s3.ts";
