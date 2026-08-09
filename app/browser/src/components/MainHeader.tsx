import React, { useEffect, useState } from "react";

import { JobControlButton } from "/@/components/header/JobControlButton";
import { getDynamicInputsCount, getOutputs } from "/@/helpers";
import { useStore } from "/@/store";
import { useJobDefinitionParam } from "/@/hooks/useJobDefinitionParam";
import { InputsRefs, JobInputs } from "/@shared/client";

import { Badge, Box, Flex, HStack, Icon, Spacer, Text, Tooltip, useMediaQuery } from "@chakra-ui/react";
import { useHashParamJson } from "@metapages/hash-query/react-hooks";
import { DownloadSimple, Gear, UploadSimple } from "@phosphor-icons/react";

import { JobStatus } from "./footer/JobStatus";

export const MainHeader: React.FC = () => {
  const [isLargerThan400] = useMediaQuery("(min-width: 400px)");
  const [jobDefinitionBlob] = useJobDefinitionParam();
  const [jobInputs] = useHashParamJson<JobInputs | undefined>("inputs");

  // only show the edit button if the command points to a script in the inputs
  const setRightPanelContext = useStore(state => state.setRightPanelContext);
  const rightPanelContext = useStore(state => state.rightPanelContext);
  const setMainInputFile = useStore(state => state.setMainInputFile);

  const currentJobDefinition = useStore(state => state.newJobDefinition);
  const incomingInputsCount = getDynamicInputsCount(currentJobDefinition);
  const [jobId, job] = useStore(state => state.jobState);
  const [outputs, setOutputs] = useState<InputsRefs>(EmptyOutputs);

  useEffect(() => {
    let cancelled = false;
    if (jobId && job) {
      (async () => {
        const newOutputs = await getOutputs(jobId, job);
        if (!cancelled) {
          setOutputs(newOutputs);
        }
      })();
    } else {
      setOutputs(EmptyOutputs);
    }
    return () => {
      cancelled = true;
    };
  }, [jobId, job]);

  const outputsCount = Object.keys(outputs).length;

  useEffect(() => {
    // TODO: make the primary editable file something that can he
    // specified so we don't rely on lexicographical order
    const fileNames = jobInputs ? Object.keys(jobInputs).sort() : [];
    const mainFile = null;
    if (!mainFile && fileNames.length) {
      setMainInputFile(fileNames[0]);
    }
  }, [jobInputs, jobDefinitionBlob]);

  const icon = (svg: React.ElementType, context: string, badge?: string) => {
    const toggleValue = rightPanelContext === context ? null : context;
    // Determine rotation based on context
    const transform = context === "inputs" ? "rotate(-90deg)" : context === "outputs" ? "rotate(90deg)" : undefined;

    return (
      <Box position="relative" display="inline-block">
        <Tooltip label={`${context[0].toUpperCase() + context.slice(1, context.length)}`}>
          <Icon
            _hover={{ bg: "gray.300" }}
            bg={context === rightPanelContext ? "gray.300" : "none"}
            p={"3px"}
            borderRadius={5}
            as={svg}
            boxSize="7"
            transform={transform}
            transition="transform 0.2s"
            onClick={() => setRightPanelContext(toggleValue)}
          />
        </Tooltip>
        {badge ? (
          <Badge
            position="absolute"
            bottom="0"
            right="0"
            transform="translate(40%, 20%)"
            colorScheme="green"
            borderRadius="full"
            boxSize="1rem">
            <Text align={"center"} fontSize={"0.7rem"}>
              {badge}
            </Text>
          </Badge>
        ) : null}
      </Box>
    );
  };

  const rightSectionWidth = isLargerThan400 ? "11rem" : "0rem";
  return (
    <Flex w={"100%"} h="55px" bg={"gray.100"} borderBottom={"1px"}>
      <HStack justify={"space-between"} px={2} w={`calc(100% - ${rightSectionWidth})`}>
        <JobStatus />
        {/* <EditInput /> */}
        <Spacer />
        <HStack>
          <JobControlButton />
        </HStack>
      </HStack>
      {isLargerThan400 && (
        <HStack borderLeft={"1px"} px={4} bg={"gray.100"} justifyContent={"space-around"} w={rightSectionWidth}>
          {icon(DownloadSimple, "inputs", incomingInputsCount ? incomingInputsCount.toString() : undefined)}
          {icon(Gear, "settings")}
          {icon(UploadSimple, "outputs", outputsCount ? outputsCount.toString() : undefined)}
        </HStack>
      )}
    </Flex>
  );
};

const EmptyOutputs: InputsRefs = {};
