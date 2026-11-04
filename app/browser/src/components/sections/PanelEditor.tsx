import React, { useCallback, useEffect, useRef, useState } from "react";

import { PanelContainer } from "/@/components/generic/PanelContainer";
import { PanelHeader } from "/@/components/generic/PanelHeader";
import { encodeOptions } from "/@/helpers";
import { useEditableJobFiles } from "/@/hooks/useEditableJobFiles";
import { useStore } from "/@/store";

import { MetaframeStandaloneComponent } from "@metapages/metapage-react";

export const PanelEditor: React.FC = () => {
  const [value, setValue] = useState(null);
  const [jobInputs, setJobFile] = useEditableJobFiles();
  const mainInputFile = useStore(state => state.mainInputFile);
  const setMainInputFileContent = useStore(state => state.setMainInputFileContent);

  // clear the main input file content on unmount
  useEffect(() => {
    return () => {
      setMainInputFileContent(null);
    };
  }, [setMainInputFileContent]);

  const options = useRef("");

  useEffect(() => {
    if (!mainInputFile) return;
    const fileExtension = mainInputFile.split(".").pop();
    options.current = encodeOptions({
      autosend: true,
      hidemenuififrame: true,
      mode: fileExtension || "sh",
      theme: "mf-default",
    });
    setValue(jobInputs[mainInputFile]);
  }, [jobInputs, mainInputFile]);

  const updateInput = useCallback(
    (content: string) => {
      setJobFile(mainInputFile, content);
    },
    [setJobFile, mainInputFile],
  );

  const onSave = useCallback(() => {
    updateInput(value);
    setMainInputFileContent(null);
  }, [value, updateInput]);

  const onOutputs = useCallback(
    // eslint-disable-next-line
    (outputs: any) => {
      if (outputs["text"] === undefined || outputs["text"] === null) {
        return;
      }
      const newValue = outputs["text"];
      if (jobInputs?.[mainInputFile] === newValue) {
        return;
      }
      setValue(newValue);
      setMainInputFileContent(newValue);
    },
    [mainInputFile, jobInputs, setMainInputFileContent],
  );
  if (!options.current) return <></>;
  return (
    <PanelContainer>
      <PanelHeader title={mainInputFile} preserveCase={true} onSave={onSave} />
      <MetaframeStandaloneComponent
        url={`https://editor.mtfm.io/#?hm=disabled&options=${options.current}`}
        inputs={{ text: value }}
        onOutputs={onOutputs}
        style={{
          minHeight: `calc(100vh - 3rem)`,
          width: "100%",
        }}
      />
    </PanelContainer>
  );
};
