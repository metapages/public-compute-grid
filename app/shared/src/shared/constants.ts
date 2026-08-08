import { ms } from "ms";

export const JobDataCacheDurationMilliseconds = ms("4 weeks") as number;

/**
 * Build and run logs are a debugging aid, not a result. They are far chattier
 * than the job record itself, so they expire well before it does.
 */
export const JobLogsDurationMilliseconds = ms("1 week") as number;
