/**
 * Via Context provide the current docker job definition which is combined from metaframe inputs
 * and URL query parameters, and the means to change (some of) them
 */
import { useEffect } from "react";

import {
  copyLargeBlobsToCloud,
  DataRefType,
  dataUrlToDataRef,
  DefaultNamespace,
  DockerJobControlConfig,
  DockerJobDefinitionInputRefs,
  DockerJobDefinitionMetadata,
  InputsRefs,
  isDataRef,
  isJobName,
  isParentInjectedHashParamKey,
  JobInputs,
  prepareEnqueueJob,
  shaDockerJob,
} from "/@shared/client";

import { getHashParamsFromWindow } from "@metapages/hash-query";
import { useHashParam, useHashParamBoolean, useHashParamJson } from "@metapages/hash-query/react-hooks";
import { DataRefSerialized, Metaframe } from "@metapages/metapage";
import { useMetaframeAndInput } from "@metapages/metapage-react";

import { getIOBaseUrl } from "../config";
import { useStore } from "../store";
import { useHashParamJsonObject } from "./useHashParamJsonObject";
import { useJobDefinitionParam, useShortJobStore } from "./useJobDefinitionParam";
import { useOptionAllowSetJob } from "./useOptionAllowSetJob";
import { useQueue } from "./useQueue";

// make sure these are all in
// app/browser/public/metaframe.json
const HashParamKeysSystem = new Set([
  "allowsetjob",
  "autostart",
  "config",
  "control",
  "debug",
  "definition",
  "files",
  "i",
  "ignoreQueueOverride",
  "job",
  "maxJobDuration",
  "queue",
  "queueOverride",
  "resolverefs",
  "terminal",
]);

const getNonSystemHashParams = (): Record<string, string> => {
  const [_, hashParams] = getHashParamsFromWindow();
  // never inject system params.
  HashParamKeysSystem.forEach(key => {
    delete hashParams[key];
  });
  // never inject params an embedding parent page (metapage.io) added: they are
  // plumbing, not container config, and several vary per page load or per viewer.
  // Deliberately not in app/browser/public/metaframe.json — that list is the
  // allowlist metapage.io uses to decide what to persist, and these are runtime only.
  Object.keys(hashParams)
    .filter(isParentInjectedHashParamKey)
    .forEach(key => {
      delete hashParams[key];
    });
  return hashParams;
};

/**
 * Gets the configuration from 1) the URL hash parameters and 2) the metaframe inputs,
 * combines them together, and sets the docker job definition in the store
 */
export const useDockerJobDefinition = () => {
  const { resolvedQueue } = useQueue();

  // TODO: unclear if this does anything anymore
  const [debug] = useHashParamBoolean("debug");
  const [maxJobDuration] = useHashParam("maxJobDuration");
  // we listen to the job parameters changing. This reads from either the
  // embedded hash (`#?job=`) or a short URL (`/j/<id>`); the setter reverts a
  // short URL to the embedded form on edit. See useJobDefinitionParam.
  const [definitionParamsInUrl, setDefinitionParamsInUrl] = useJobDefinitionParam();
  const shortJobId = useShortJobStore(s => s.shortJobId);

  // job files edited in the page are stored in the URL hash
  const [jobFilesFromUrl] = useHashParamJsonObject<JobInputs>("files");
  // the inputs for this run: `/j/<job>#?i=<json of name: text or DataRef>`
  const [runInputsFromUrl] = useHashParamJsonObject<Record<string, unknown>>("i");

  // Parent pages to this frame can inject a userspace config
  // to uniquely identify the user and limit the job to a single userspace
  // and provide webhooks
  const [namespaceConfig] = useHashParamJson<DockerJobControlConfig | undefined>("control");

  // this changes when the metaframe inputs change
  const metaframeBlob = useMetaframeAndInput();
  // important: do NOT auto serialize input blobs, since the worker is
  // the only consumer, it wastes resources
  // Output blobs tho?
  useEffect(() => {
    // This is here but currently does not seem to work:
    // https://github.com/metapages/metapage/issues/117
    if (metaframeBlob?.metaframe) {
      metaframeBlob.metaframe.isInputOutputBlobSerialization = false;
    }
  }, [metaframeBlob?.metaframe]);

  const [allowSetJob] = useOptionAllowSetJob();
  // allow setting the job definition params in the url hash via the "metaframe/job" input
  useEffect(() => {
    if (!allowSetJob || !metaframeBlob?.inputs?.["job"]) {
      return;
    }
    const jobInput = metaframeBlob.inputs["job"];
    setDefinitionParamsInUrl(jobInput);
  }, [allowSetJob, metaframeBlob.inputs, setDefinitionParamsInUrl]);

  // When all the things are updated, set the new job definition
  const setNewJobDefinition = useStore(state => state.setNewJobDefinition);

  // if the URL inputs change, or the metaframe inputs change, maybe update the store.newJobDefinition
  useEffect(() => {
    let cancelled = false;
    // So convert all possible input data types into datarefs for smallest internal representation (no big blobs)
    const definition: DockerJobDefinitionInputRefs = {
      ...definitionParamsInUrl,
    };

    const hashParams = getNonSystemHashParams();

    definition.env = {
      ...definition.env,
      ...hashParams,
    };

    // inject any hash params into the definition
    if (definition.command) {
      Object.keys(hashParams).forEach(key => {
        definition.command = definition.command.replace("${" + key + "}", hashParams[key]);
      });
    }
    if (definition.build?.context) {
      Object.keys(hashParams).forEach(key => {
        definition.build.context = definition.build.context.replace("${" + key + "}", hashParams[key]);
      });
    }

    // These are always type: DataRefType.utf8 because they come from the text editor.
    // They add to, or replace, the files of a definition loaded from a short URL.
    definition.files = {
      ...definition.files,
      ...Object.fromEntries(
        Object.keys(jobFilesFromUrl ?? {}).map(key => [
          key,
          { type: DataRefType.utf8, value: jobFilesFromUrl[key] as string },
        ]),
      ),
    };

    if (!definition.image && !definition.build) {
      return;
    }

    // These are not part of the job: they make this a run of the job on these inputs
    let runInputs: InputsRefs = {};

    (async () => {
      if (cancelled) {
        return;
      }
      // convert inputs into internal data refs so workers can consume
      // Get ALL inputs, not just the most recent, since inputs come
      // in from different sources at different times, and we accumulate them
      let inputs = { ...runInputsFromUrl, ...(metaframeBlob?.metaframe?.getInputs() || {}) };
      // This is a special input, it's the job definition, not a file input
      delete inputs["job"];

      // TODO: this shouldn't be needed, but there is a bug:
      // https://github.com/metapages/metapage/issues/117
      // This converts blobs and files into base64 strings
      inputs = await Metaframe.serializeInputs(inputs);
      if (cancelled) {
        return;
      }
      Object.keys(inputs).forEach(name => {
        const fixedName = name.startsWith("/") ? name.slice(1) : name;
        const value = inputs[name];
        // null (and undefined) cannot be serialized, so skip them
        if (value === undefined || value === null) {
          return;
        }
        if (typeof value === "object" && value?._s === true) {
          const blob = value as DataRefSerialized;
          // serialized blob/typedarray/arraybuffer
          runInputs[fixedName] = {
            value: blob.value,
            type: DataRefType.base64,
          };
        } else {
          // metapage.io sends v2 datarefs: data URL strings, not {type,value}
          // objects. `isDataRef` only matches the v1 object shape, so without
          // this the string falls through to the utf8 branch below and the
          // container gets the literal "data:text/x-uri;..." text as its input.
          const dataRefFromDataUrl = dataUrlToDataRef(value);
          // If it's a DataRef, just use it, then there's
          // no need to serialize it, or further process
          if (dataRefFromDataUrl) {
            runInputs[fixedName] = dataRefFromDataUrl;
          } else if (isDataRef(value)) {
            runInputs[fixedName] = value;
          } else if (typeof value === "object") {
            // assume object is a json object
            runInputs[fixedName] = {
              value,
              type: DataRefType.json,
            };
          } else if (typeof value === "string") {
            runInputs[fixedName] = {
              value,
              type: DataRefType.utf8,
            };
          } else if (typeof value === "number") {
            runInputs[fixedName] = {
              value: `${value}`,
              type: DataRefType.utf8,
            };
          } else {
            console.error(`I don't know how to handle input ${name}:`, value);
          }
          // TODO: is this true: Now all (non-blob) values are DataMode.utf8
        }
      });

      // at this point, these inputs *could* be very large blobs.
      // any big things are uploaded to cloud storage, then the input is replaced with a reference to the cloud lump
      const ioBaseUrl = getIOBaseUrl(resolvedQueue);

      runInputs = await copyLargeBlobsToCloud(runInputs, ioBaseUrl);
      if (cancelled) {
        return;
      }
      definition.files = await copyLargeBlobsToCloud(definition.files, ioBaseUrl);
      if (cancelled) {
        return;
      }

      // if uploading a large blob means new inputs have arrived and replaced this set, break out
      // Opened by name: runs stay tied to the name. Only when this is the name's
      // latest version unchanged, since submitting under a name publishes to it,
      // and pinning an old version or editing params in the URL must not do that.
      const searchParams = new URLSearchParams(window.location.search);
      const isLatestOfName =
        !!shortJobId &&
        isJobName(shortJobId) &&
        !searchParams.has("v") &&
        !searchParams.has("i") &&
        (await shaDockerJob(definitionParamsInUrl)) === (await shaDockerJob(definition));

      const {
        id,
        definition: definitionPrepared,
        jobId,
        jobHash,
        inputsHash,
        inputs: inputsPrepared,
      } = await prepareEnqueueJob({
        definition,
        inputs: runInputs,
        jobId: isLatestOfName ? shortJobId : undefined,
      });
      if (cancelled) {
        return;
      }

      const newJobDefinition: DockerJobDefinitionMetadata = {
        hash: id,
        jobId,
        jobHash,
        inputsHash,
        inputs: inputsPrepared,
        definition: definitionPrepared,
        debug,
        maxJobDuration,
        control: { ...namespaceConfig, namespace: namespaceConfig?.namespace || DefaultNamespace },
      };
      if (debug) {
        console.log("container.mtfm.io DEBUG: newJobDefinition", newJobDefinition);
      }

      setNewJobDefinition(newJobDefinition);
    })();

    return () => {
      cancelled = true;
    };
  }, [
    resolvedQueue,
    metaframeBlob.inputs,
    definitionParamsInUrl,
    shortJobId,
    jobFilesFromUrl,
    runInputsFromUrl,
    namespaceConfig,
    debug,
    maxJobDuration,
  ]);
};
