// Side-effect import: registers the webhook retry cron. See webhooks-cron.ts
// for why this is a module import and not a function call.
import "../../shared/src/shared/webhooks-cron.ts";

import type { DockerJobQueue } from "./queue/DockerJobQueue.ts";
import { runCommand } from "/@/commands/run.ts";
import { testCommand } from "/@/commands/test.ts";
import { processes } from "/@/processes.ts";

import { Command } from "@cliffy/command";

// Global reference to the DockerJobQueue for cleanup
let globalDockerJobQueue: DockerJobQueue | null = null;

// Function to set the global reference
export const setGlobalDockerJobQueue = (queue: DockerJobQueue) => {
  globalDockerJobQueue = queue;
};

const args = Deno.args;

// running in docker doesn't automatically kill on ctrl-c
// https://github.com/nodejs/node/issues/4182
Deno.addSignalListener("SIGINT", () => {
  console.log("SIGINT Cleaning up processes...");
  if (globalDockerJobQueue) {
    globalDockerJobQueue.stopPeriodicRegistration();
  }
  if (processes.dockerd) {
    processes.dockerd.kill("SIGINT");
  }
  Deno.exit(0);
});

Deno.addSignalListener("SIGTERM", () => {
  console.log("SIGTERM Cleaning up processes...");
  if (globalDockerJobQueue) {
    globalDockerJobQueue.stopPeriodicRegistration();
  }
  if (processes.dockerd) {
    processes.dockerd.kill("SIGTERM");
  }
  Deno.exit(0);
});

await new Command()
  .description("Commands for running a metapage worker")
  .name("worker")
  .version("v1.0.0")
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
  // end sub-commands
  .reset()
  .action(function () {
    this.showHelp();
  })
  .parse(args);
