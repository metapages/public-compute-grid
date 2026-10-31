import { useCallback, useMemo } from "react";

import { useHashParamJsonObject } from "/@/hooks/useHashParamJsonObject";
import { JobInputs } from "/@shared/client";

import { useHashParamJson } from "@metapages/hash-query/react-hooks";

/**
 * The text files editable in the page, kept in the URL hash: job files (`files`)
 * and, for existing URLs, the deprecated `inputs`. An edit goes back to the param
 * the file came from.
 */
export const useEditableJobFiles = (): [JobInputs, (name: string, content: string) => void] => {
  const [jobFiles, setJobFiles] = useHashParamJsonObject<JobInputs>("files");
  const [jobInputs, setJobInputs] = useHashParamJson<JobInputs | undefined>("inputs");

  const files = useMemo(() => ({ ...jobInputs, ...jobFiles }), [jobInputs, jobFiles]);

  const setFile = useCallback(
    (name: string, content: string) => {
      if (jobInputs?.[name] !== undefined && jobFiles?.[name] === undefined) {
        setJobInputs({ ...jobInputs, [name]: content });
      } else {
        setJobFiles({ ...jobFiles, [name]: content });
      }
    },
    [jobFiles, jobInputs, setJobFiles, setJobInputs],
  );

  return [files, setFile];
};
