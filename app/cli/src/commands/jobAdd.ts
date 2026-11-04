import { exists } from "std/fs";
import { readAll, writeAllSync } from "std/io";
import { basename } from "std/path";

import { Command } from "@cliffy/command";
import { closed, open } from "@korkje/wsi";
import {
  type BroadcastJobStates,
  createNewContainerJobMessage,
  type DockerJobDefinitionInputRefs,
  DockerJobState,
  fileToDataref,
  finishedJobOutputsToFiles,
  type InMemoryDockerJob,
  type InputsRefs,
  type StateChangeValueFinished,
  type WebsocketMessageServerBroadcast,
  WebsocketMessageTypeServerBroadcast,
} from "@metapages/compute-queues-shared";

export const jobAdd = new Command()
  .arguments("<queue:string> [stdin:string]")
  .description("Add a job to a queue")
  .option("-m, --image [image:string]", "Docker image", {
    default: "alpine:latest",
    conflicts: ["git"],
  })
  .option("-f, --file [file:string]", "Input files", { collect: true })
  .option("-r, --git [git:string]", "Git repo source")
  .option("-c, --command [command:string]", "Container command")
  .option(
    "-w, --wait [wait:boolean]",
    "Wait until job is finished before returning",
  )
  .option(
    "-o, --outputs [outputs:string]",
    "Directory to copy output files (if any)",
    {
      default: "./outputs",
    },
  )
  .option("--debug", "Debug flag for slower running but more logging")
  // .option("-g, --gpu [gpu:boolean]", "Enable GPU access", { default: false })
  .action(
    (
      options: {
        image?: string | undefined | boolean;
        command?: string | undefined | boolean;
        git?: string | undefined | boolean;
        debug?: string | undefined | boolean;
        wait?: string | undefined | boolean;
        outputs?: string | undefined | boolean;
        file?: (string | boolean)[] | undefined;
        apiServerAddress?: string | undefined;
      },
      queue: string,
      stdin?: string,
    ) => {
      (async () => {
        const {
          image,
          command,
          git,
          debug,
          apiServerAddress,
          file: files,
          wait,
          outputs,
        } = options;

        const address = apiServerAddress ||
          globalThis?.location?.origin ||
          "https://container.mtfm.io";
        const url = `${address}/${queue}/client`;

        const imageOrGit: string = (git as string) || (image as string) ||
          "alpine:latest";
        let definition: DockerJobDefinitionInputRefs = {
          image: imageOrGit,
          command: command as string,
          inputs: {},
        };

        if (stdin === "-") {
          const decoder = new TextDecoder();
          const input = decoder.decode(await readAll(Deno.stdin));
          definition = JSON.parse(input);
        }

        // handle input files
        const inputs: InputsRefs = {};
        if (files) {
          for (const file of files) {
            if (typeof file !== "string") {
              continue;
            }
            const fileExists = await exists(file);
            if (!fileExists) {
              throw `File does not exist: ${file}`;
            }
            // TODO: handle BIG blobs
            const ref = await fileToDataref(file, address);
            inputs[basename(file)] = ref;
          }
        }

        const { message, jobId /*, stageChange */ } = await createNewContainerJobMessage({
          definition,
          inputs,
          debug: !!debug,
        });

        const {
          promise: jobQueuedOrCompleteDeferred,
          resolve,
          reject,
        } = Promise.withResolvers<InMemoryDockerJob>();

        const socket = new WebSocket(`${url.replace("http", "ws")}`);

        socket.addEventListener("error", (event) => {
          writeAllSync(
            Deno.stderr,
            new TextEncoder().encode("💥 WebSocket error:"),
          );
          writeAllSync(
            Deno.stderr,
            new TextEncoder().encode(`${event}`),
          );
          reject(event);
        });
        let resolved = false;
        socket.onmessage = (message: MessageEvent) => {
          if (resolved) {
            return;
          }

          const messageString = message.data.toString();
          // console.log('messageString', messageString);
          const possibleMessage: WebsocketMessageServerBroadcast = JSON.parse(
            messageString,
          );
          switch (possibleMessage.type) {
            case WebsocketMessageTypeServerBroadcast.JobStates:
            case WebsocketMessageTypeServerBroadcast.JobStateUpdates: {
              const someJobsPayload = possibleMessage
                .payload as BroadcastJobStates;
              if (!someJobsPayload) {
                break;
              }
              const jobDefinitionRow = someJobsPayload.state.jobs[jobId];
              if (!jobDefinitionRow) {
                break;
              }

              // if we are NOT waiting, return on ANY job state change
              // since that means the job is AT LEAST submitted
              // TODO: question: error states?
              if (!wait) {
                resolve(jobDefinitionRow);
                resolved = true;
                return;
              }
              if (jobDefinitionRow.state === DockerJobState.Finished) {
                // const finishedState = jobDefinitionRow
                //   .value as StateChangeValueFinished;
                (async () => {
                  const finishedState =
                    await (await fetch(`${address}/${queue}/j/${jobId}/result.json`, { redirect: "follow" }))
                      .json() as StateChangeValueFinished;
                  await finishedJobOutputsToFiles(
                    finishedState,
                    outputs as string,
                    address,
                  );
                  resolved = true;
                  resolve(jobDefinitionRow);
                })();
              }
              break;
            }
            default:
              //ignored
          }
        };

        // or, sychronously
        writeAllSync(
          Deno.stderr,
          new TextEncoder().encode("Opening socket..."),
        );
        await open(socket);
        writeAllSync(Deno.stderr, new TextEncoder().encode("✅\n"));
        socket.send(JSON.stringify(message));

        writeAllSync(
          Deno.stderr,
          new TextEncoder().encode("Awaiting on job to be queued...\n"),
        );

        const result = await jobQueuedOrCompleteDeferred;

        console.log(JSON.stringify(result));

        socket.close();
        await closed(socket);
        // writeAllSync(Deno.stderr, new TextEncoder().encode("Socket closed ✅\n"));
      })();
    },
  );
