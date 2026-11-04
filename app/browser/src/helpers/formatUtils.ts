import { DockerJobDefinitionMetadata, DockerJobState, InMemoryDockerJob, InputsRefs } from "/@shared/client";
import stringify from "safe-stable-stringify";

import { cache } from "../cache";

// eslint-disable-next-line
export const encodeOptions = (options: any): string => {
  const text: string = stringify(options) || "";
  const b64 = btoa(encodeURIComponent(text));
  return b64;
};

export const capitalize = (str: string): string => {
  if (!str?.length) return str;
  return str[0].toUpperCase() + str.slice(1, str.length);
};

export const getDynamicInputsCount = (currentJobDefinition: DockerJobDefinitionMetadata | undefined) => {
  return Object.keys({ ...currentJobDefinition?.definition?.inputs, ...currentJobDefinition?.inputs }).length;
};

export const getDynamicInputs = (currentJobDefinition: DockerJobDefinitionMetadata | undefined): InputsRefs => {
  return { ...currentJobDefinition?.definition?.inputs, ...currentJobDefinition?.inputs };
};

export const getOutputs = async (jobId: string, job: InMemoryDockerJob): Promise<InputsRefs> => {
  if (!job?.state || job.state !== DockerJobState.Finished) {
    return Promise.resolve(EmptyOutputs);
  }
  const finishedJob = await cache.getFinishedJob(jobId);
  return finishedJob?.finished?.result?.outputs || EmptyOutputs;
};

const EmptyOutputs: InputsRefs = {};
