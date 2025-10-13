import React, { useCallback } from "react";

import { ApiOrigin } from "/@/config";
import { useQueue } from "/@/hooks/useQueue";
import { useStore } from "/@/store";
import {
  ConsoleLogLine,
  DockerJobFinishedReason,
  DockerJobState,
  InMemoryDockerJob,
  StateChangeValueFinished,
} from "/@shared/client";
import humanizeDuration from "humanize-duration";

import { Box, HStack, Icon, Link, Text, useToast, VStack } from "@chakra-ui/react";
import { Check, Circle, HourglassMedium, Prohibit, WarningCircle } from "@phosphor-icons/react";

import { useMinimalHeader } from "../../hooks/useMinimalHeader";
import { useActiveJobsCount } from "../../hooks/useActiveJobsCount";

const humanizeDurationOptions = {
  // round: true,
  maxDecimalPoints: 1,
};

const STATUS_ICON_SIZE = 6;
export const JobStatus: React.FC = () => {
  const toast = useToast();
  const { resolvedQueue } = useQueue();
  const isMinimalHeader = useMinimalHeader();

  const workers = useStore(state => state.workers);
  const [_, job] = useStore(state => state.jobState);
  const newJobDefinition = useStore(state => state.newJobDefinition);
  const jobId = newJobDefinition?.hash;
  const buildLogs = useStore(state => state.buildLogs);
  const setRightPanelContext = useStore(state => state.setRightPanelContext);
  const rightPanelContext = useStore(state => state.rightPanelContext);
  const activeJobsCount = useActiveJobsCount();

  const resultsFinished = job?.finished;

  const { icon, text, exitCode, showExitCodeRed, ...rest } = getJobStateValues(
    jobId,
    job,
    resultsFinished,
    workers?.workers?.length || 0,
    buildLogs,
  );
  const { workerCount } = rest;
  let { desc } = rest;

  const copyJobId = useCallback(() => {
    // Note: this does not currently work
    // see https://www.chromium.org/Home/chromium-security/deprecating-permissions-in-cross-origin-iframes/
    navigator?.clipboard?.writeText(jobId);

    // eslint-disable-next-line
    if (!!navigator?.clipboard?.writeText) {
      toast({
        position: "bottom-left",
        duration: 20,
        isClosable: true,
        render: () => (
          <Box color="gray.600" p={3} bg="gray.300" mb={"footerHeight"}>
            <Text>Job Id copied to clipboard</Text>
          </Box>
        ),
      });
    }
  }, [jobId, toast]);

  const openQueuePanel = useCallback(() => {
    if (rightPanelContext === "queue") {
      setRightPanelContext(null);
    } else {
      setRightPanelContext("queue");
    }
  }, [rightPanelContext, setRightPanelContext]);

  if (desc && desc.includes(" exec: ")) {
    desc = desc.split(" exec: ")[1];
  } else if (desc) {
    desc = "..." + desc?.substring(desc.length - 60);
  }

  if (!jobId) {
    return <></>;
  }

  if (!resolvedQueue) return <></>;

  return (
    <HStack h={"100%"} gap={5} alignItems="center" justifyContent={"center"}>
      {icon}
      <VStack gap={0.2} alignItems={"flex-start"}>
        <HStack gap={1} alignItems={"baseline"}>
          <Text align={"start"} fontWeight={500} noOfLines={1}>
            {text}
          </Text>
          {workerCount !== undefined && (
            <Text
              ml="0.5rem"
              fontSize={"0.85rem"}
              cursor={"pointer"}
              onClick={openQueuePanel}
              _hover={{ textDecoration: "underline" }}>
              ({activeJobsCount}/{workerCount})
            </Text>
          )}
        </HStack>

        {!isMinimalHeader && jobId && (
          <Link href={`${resolvedQueue === "local" ? "http://localhost:8000" : ApiOrigin}/j/${jobId}`} isExternal>
            <Text cursor={"copy"} onClick={copyJobId} fontSize={"0.7rem"}>
              Id: {jobId.slice(0, 5)}
            </Text>
          </Link>
        )}
        {!isMinimalHeader && exitCode && (
          <Link>
            <Text color={showExitCodeRed ? "red" : undefined} fontSize={"0.7rem"}>
              Exit Code: {exitCode}
            </Text>
          </Link>
        )}
        {desc && !workerCount && <Text fontSize={"0.7rem"}>{desc}</Text>}
      </VStack>
    </HStack>
  );
};

const getJobStateValues = (
  jobId: string | undefined,
  job: InMemoryDockerJob | undefined,
  resultFinished: StateChangeValueFinished | undefined,
  workerCount: number,
  buildLogs: ConsoleLogLine[] | null,
) => {
  let text = "";
  let icon = <></>;
  let desc = null;
  let exitCode = null;
  let showExitCodeRed = false;
  let returnWorkerCount: number | undefined = undefined;
  const state = job?.state;

  const errorBlob = resultFinished?.result?.error as { statusCode: number; json: { message: string } } | undefined;

  if (!job) {
    text = "-"; //"No job started";
    icon = <Icon as={Prohibit} boxSize={STATUS_ICON_SIZE} opacity={0} />;
  }

  switch (state) {
    case DockerJobState.Finished:
      if (!resultFinished) {
        icon = <Icon color={"red"} as={WarningCircle} boxSize={STATUS_ICON_SIZE} />;
        text = "Job Finished - No Result";
        showExitCodeRed = true;
        break;
      }
      switch (resultFinished.reason) {
        case DockerJobFinishedReason.Cancelled:
        case DockerJobFinishedReason.Deleted:
        case DockerJobFinishedReason.JobReplacedByClient:
          text = "-"; //"No job started";
          icon = <Icon as={Prohibit} boxSize={STATUS_ICON_SIZE} opacity={0} />;
          // icon = <Icon as={WarningCircle} boxSize={STATUS_ICON_SIZE} />;
          // text = `Job Cancelled ${resultFinished?.result?.duration ? `(${humanizeDuration(resultFinished.result.duration, humanizeDurationOptions)})` : ""}`;
          break;
        case DockerJobFinishedReason.Error:
          showExitCodeRed = true;
          icon = <Icon color={"red"} as={WarningCircle} boxSize={STATUS_ICON_SIZE} />;
          text = `Job Failed ${resultFinished?.result?.duration ? `(${humanizeDuration(resultFinished.result.duration, humanizeDurationOptions)})` : ""}`;
          // truncate to char len, add modal if it's longer than one line (to right of exit code)
          desc = errorBlob?.json?.message;
          exitCode = errorBlob?.statusCode;
          break;
        case DockerJobFinishedReason.Success:
          exitCode = resultFinished?.result?.StatusCode;
          text = `Job Complete ${resultFinished?.result?.duration ? `(${humanizeDuration(resultFinished.result.duration, humanizeDurationOptions)})` : ""}`;
          if (exitCode === 0) {
            icon = <Icon color={"green"} as={Check} boxSize={STATUS_ICON_SIZE} />;
          } else {
            icon = <Icon color={"orange"} as={WarningCircle} boxSize={STATUS_ICON_SIZE} />;
          }
          break;
        case DockerJobFinishedReason.TimedOut:
          icon = <Icon color={"red"} as={WarningCircle} boxSize={STATUS_ICON_SIZE} />;
          text = `Job Timed Out ${resultFinished?.result?.duration ? `(${humanizeDuration(resultFinished.result.duration, humanizeDurationOptions)})` : ""}`;
          break;
        case DockerJobFinishedReason.WorkerLost:
          icon = <Icon color={"red"} as={WarningCircle} boxSize={STATUS_ICON_SIZE} />;
          text = "Connection with worker lost, waiting to requeue";
          break;
      }
      break;
    case DockerJobState.Queued:
      icon = <Icon as={HourglassMedium} boxSize={STATUS_ICON_SIZE} />;
      text = "Job Queued";
      break;
    case DockerJobState.Running:
      text = buildLogs && buildLogs.length > 0 ? "Job Building" : "Job Running";
      icon = <Icon as={Circle} color={"orange"} boxSize={STATUS_ICON_SIZE} />;
      returnWorkerCount = workerCount;
      break;
    case DockerJobState.Removed:
      text = "-";
      icon = <Icon as={Prohibit} boxSize={STATUS_ICON_SIZE} opacity={0} />;
      break;
  }
  return { text, icon, desc, exitCode, jobId, showExitCodeRed, workerCount: returnWorkerCount };
};
