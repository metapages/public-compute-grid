import { useCallback, useMemo } from "react";

import { useHashParamJsonObject } from "/@/hooks/useHashParamJsonObject";
import { useJobDefinitionParam } from "/@/hooks/useJobDefinitionParam";
import { DataRefType, JobInputs } from "/@shared/client";

/**
 * The text files editable in the page: the definition's own text files, and those
 * in the URL hash (`files`). An edit goes to the hash, and wins over the definition.
 */
export const useEditableJobFiles = (): [JobInputs, (name: string, content: string) => void] => {
  const [definition] = useJobDefinitionParam();
  const [jobFiles, setJobFiles] = useHashParamJsonObject<JobInputs>("files");

  const files = useMemo(() => {
    const definitionFiles = Object.entries(definition?.files ?? {})
      .filter(([_, ref]) => ref.type === DataRefType.utf8)
      .map(([name, ref]) => [name, ref.value as string]);
    return { ...Object.fromEntries(definitionFiles), ...jobFiles };
  }, [definition, jobFiles]);

  const setFile = useCallback(
    (name: string, content: string) => {
      setJobFiles({ ...jobFiles, [name]: content });
    },
    [jobFiles, setJobFiles],
  );

  return [files, setFile];
};
