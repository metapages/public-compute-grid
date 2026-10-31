import { Box, Container, HStack, useMediaQuery, VStack } from "@chakra-ui/react";
import React from "react";

import { useStore } from "../store";

import { useMinimalHeader } from "../hooks/useMinimalHeader.tsx";
import { JobStatus } from "/@/components/footer/JobStatus";
import { JobControlButton } from "/@/components/header/JobControlButton";
import { MainFooter } from "/@/components/MainFooter";
import { MainHeader } from "/@/components/MainHeader";
import { ShortJobNotFound } from "/@/components/ShortJobNotFound";
import { PanelDocs } from "/@/components/sections/PanelDocs";
import { PanelEditor } from "/@/components/sections/PanelEditor";
import { PanelInputs } from "/@/components/sections/PanelInputs";
import { PanelLogs } from "/@/components/sections/PanelLogs";
import { PanelOutputs } from "/@/components/sections/PanelOutputs";
import { PanelQueue } from "/@/components/sections/PanelQueue";
import { PanelSettings } from "/@/components/sections/PanelSettings";

export const Main: React.FC = () => {
  const rightPanelContext = useStore(state => state.rightPanelContext);
  const [isWiderThan1000] = useMediaQuery("(min-width: 1000px)");
  const isMinimalHeader = useMinimalHeader();

  const editorShown = rightPanelContext === "editScript";
  const stdErrShown = rightPanelContext === "stderr";
  const rightPanelOptions = {
    inputs: <PanelInputs />,
    outputs: <PanelOutputs />,
    settings: <PanelSettings />,
    editScript: <PanelEditor />,
    help: <PanelDocs />,
    stderr: <PanelLogs mode={"stderr"} />,
    queue: <PanelQueue />,
  };
  const rightContent = rightPanelContext && rightPanelOptions[rightPanelContext];
  let rightWidth = "0%";
  if (rightPanelContext) {
    if (!isWiderThan1000) {
      if (editorShown) {
        rightWidth = "100%";
      } else if (stdErrShown) {
        rightWidth = "50%";
      } else {
        rightWidth = "80%";
      }
    } else {
      rightWidth = "50%";
    }
  }
  const leftWidth = `calc(100% - ${rightWidth})`;

  if (isMinimalHeader) {
    return (
      <Container
        m={0}
        // bg={"gray.300"}
        minW={"100%"}
        minHeight="100vh"
        height="100vh"
        w={"100vw"}
        border="1px solid #E4E4E4"
        boxSizing="border-box">
        <HStack justifyContent="space-between" h="100%" alignItems="center" boxSizing="border-box">
          <JobStatus />
          <JobControlButton />
        </HStack>
      </Container>
    );
  }
  return (
    <VStack
      gap={0}
      minWidth={"200px"}
      minHeight="100vh"
      maxHeight="100vh"
      border="1px solid #E4E4E4"
      boxSizing="border-box">
      <MainHeader />
      <ShortJobNotFound />
      <HStack gap={0} w={"100%"} minW="100%" minH={"contentHeight"}>
        <Box minW={leftWidth} minH={"contentHeight"}>
          <PanelLogs mode={stdErrShown ? "stdout" : "stdout+stderr"} />
        </Box>
        <Box minW={rightWidth} minH={"contentHeight"} borderLeft={rightContent && "1px"} boxSizing="border-box">
          {rightContent}
        </Box>
      </HStack>
      <MainFooter />
    </VStack>
  );
};
