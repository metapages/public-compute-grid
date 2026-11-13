// Side-effect import: registers the webhook retry cron. See webhooks-cron.ts
// for why this is a module import and not a function call.
import "../../shared/src/shared/webhooks-cron.ts";

import type { DockerJobQueue } from "./queue/DockerJobQueue.ts";
import { runCommand } from "/@/commands/run.ts";
import { testCommand } from "/@/commands/test.ts";
import { processes } from "/@/processes.ts";
import { userJobQueues } from "@metapages/compute-queues-shared";

import { Command } from "@cliffy/command";

import mod from "../mod.json" with { type: "json" };

// Global reference to the DockerJobQueue for cleanup
let globalDockerJobQueue: DockerJobQueue | null = null;

/**
 * In local mode this process is also the server: write out the live job logs
 * it has not spooled to disk yet, so the next process reloads all of them
 * (#360). Bounded, so a stuck disk cannot block the exit.
 */
const flushLiveLogs = () =>
  Promise.race([
    Promise.all(Object.values(userJobQueues).map((queue) => queue.flushLiveLogs())),
    new Promise((resolve) => setTimeout(resolve, 2000)),
  ]);

// Function to set the global reference
export const setGlobalDockerJobQueue = (queue: DockerJobQueue) => {
  globalDockerJobQueue = queue;
};

const args = Deno.args;

// running in docker doesn't automatically kill on ctrl-c
// https://github.com/nodejs/node/issues/4182
Deno.addSignalListener("SIGINT", async () => {
  console.log("SIGINT Cleaning up processes...");
  if (globalDockerJobQueue) {
    globalDockerJobQueue.stopPeriodicRegistration();
  }
  if (processes.dockerd) {
    processes.dockerd.kill("SIGINT");
  }
  await flushLiveLogs();
  Deno.exit(0);
});

Deno.addSignalListener("SIGTERM", async () => {
  console.log("SIGTERM Cleaning up processes...");
  if (globalDockerJobQueue) {
    globalDockerJobQueue.stopPeriodicRegistration();
  }
  if (processes.dockerd) {
    processes.dockerd.kill("SIGTERM");
  }
  await flushLiveLogs();
  Deno.exit(0);
});

await new Command()
  .description("Commands for running a metapage worker")
  .name("worker")
  .version(mod.version)
  .help({
    types: false,
    hints: false,
  })
  .action(function () {
    this.showHelp();
  })
  // Put all the sub-commands here
  .command("run", runCommand)
  .command("test", testCommand)
  .command(
    "version",
    new Command()
      .description("Print the worker release version")
      .action(() => {
        console.log(mod.version);
        // the webhook cron registered on import would keep the process alive
        Deno.exit(0);
      }),
  )
  // end sub-commands
  .reset()
  .action(function () {
    this.showHelp();
  })
  .parse(args);
