import { BaseDockerJobQueue, getEnv, userJobQueues } from "@metapages/compute-queues-shared";

export class ApiDockerJobQueue extends BaseDockerJobQueue {
  constructor(opts: { serverId: string; address: string; periodicWork?: boolean }) {
    // LIVE_LOGS_DIRECTORY: spool live job logs there so a restarted instance
    // keeps them (#360). Mount it on a volume every api instance shares. Unset
    // (Cloudflare has no filesystem): live logs are in memory only.
    super({ ...opts, liveLogsDirectory: getEnv("LIVE_LOGS_DIRECTORY") || undefined });
  }
}

export { userJobQueues };
