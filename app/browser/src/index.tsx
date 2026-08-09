import "@fontsource-variable/jetbrains-mono";
import "./styles/app.css";

import { App } from "/@/App";
import { hydrateShortJobFromPath } from "/@/hooks/useJobDefinitionParam";
import { mfTheme } from "/@/styles/theme";
import { createRoot } from "react-dom/client";

import { ChakraProvider } from "@chakra-ui/react";
import { WithMetaframeAndInputs } from "@metapages/metapage-react";

const container = document.getElementById("root");

const render = () => {
  createRoot(container!).render(
    <ChakraProvider theme={mfTheme}>
      <WithMetaframeAndInputs>
        <App />
      </WithMetaframeAndInputs>
    </ChakraProvider>,
  );
};

// When opened at a short URL (/j/<jobId>), load the definition from the server
// before rendering so the first render already has the job. For every other URL
// this resolves immediately without a fetch.
hydrateShortJobFromPath().finally(render);
