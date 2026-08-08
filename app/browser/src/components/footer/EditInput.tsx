import React, { useEffect, useRef } from "react";

import { useJobDefinitionParam } from "/@/hooks/useJobDefinitionParam";
import { JobInputs } from "/@shared/client";
import { useStore } from "/@/store";

import { Button, HStack, Icon, Text, Tooltip } from "@chakra-ui/react";
import { useHashParamJson } from "@metapages/hash-query/react-hooks";
import { PencilSimple, Terminal } from "@phosphor-icons/react";
import { useOptionShowTerminalFirst } from "/@/hooks/useOptionShowTerminalFirst";

export const EditInput: React.FC = () => {
  const [jobDefinitionBlob] = useJobDefinitionParam();
  const [jobInputs] = useHashParamJson<JobInputs | undefined>("inputs");

  // only show the edit button if the command points to a script in the inputs
  const setRightPanelContext = useStore(state => state.setRightPanelContext);
  const rightPanelContext = useStore(state => state.rightPanelContext);
  const setMainInputFile = useStore(state => state.setMainInputFile);
  const mainInputFile = useStore(state => state.mainInputFile);
  const startEditingCheckRef = useRef<boolean>(false);
  const [showTerminalFirst, _, loading] = useOptionShowTerminalFirst();

  useEffect(() => {
    // TODO: make the primary editable file something that can he
    // specified so we don't rely on lexicographical order
    const fileNames = jobInputs ? Object.keys(jobInputs).sort() : [];
    const mainFile = null;
    if (!mainFile && fileNames.length) {
      setMainInputFile(fileNames[0]);
      if (!loading && !startEditingCheckRef.current) {
        if (!showTerminalFirst) {
          setRightPanelContext("editScript");
        }
        startEditingCheckRef.current = true;
      }
    }
  }, [jobInputs, jobDefinitionBlob, showTerminalFirst, loading]);

  const editorShown = rightPanelContext === "editScript";
  return (
    <Tooltip label={editorShown ? "Close" : "Edit"}>
      <HStack>
        <Icon as={Terminal} boxSize="4" />
        {!mainInputFile ? (
          <Text fontWeight={400}>{jobDefinitionBlob?.command}</Text>
        ) : (
          <Button
            variant={"ghost"}
            bg={editorShown ? "gray.300" : "none"}
            onClick={() => setRightPanelContext(editorShown ? null : "editScript")}
            _hover={{ bg: editorShown ? "gray.300" : "none" }}>
            <HStack gap={2}>
              <Text>{`${mainInputFile}`}</Text>
              <Icon as={PencilSimple} />
            </HStack>
          </Button>
        )}
      </HStack>
    </Tooltip>
  );
};
