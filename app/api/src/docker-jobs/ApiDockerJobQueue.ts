import { BaseDockerJobQueue, userJobQueues } from "@metapages/compute-queues-shared";

export class ApiDockerJobQueue extends BaseDockerJobQueue {
  constructor(opts: { serverId: string; address: string; periodicWork?: boolean }) {
    super(opts);
  }
}

export { userJobQueues };
