import * as esbuild from "npm:esbuild@0.25.5";
import { denoPlugins } from "jsr:@luca/esbuild-deno-loader@0.11.1";
import { exists } from "std/fs";

async function bundle() {
  const distDir = "./dist";

  if (!await exists(distDir)) {
    await Deno.mkdir(distDir, { recursive: true });
  }

  try {
    await esbuild.build({
      plugins: [...denoPlugins()],
      entryPoints: ["./src/client.ts"],
      outfile: `${distDir}/client.mjs`,
      bundle: true,
      format: "esm",
      platform: "browser",
      sourcemap: true,
    });

    await new Deno.Command("deno", {
      args: [
        "run",
        "--allow-read",
        "--allow-write",
        "--allow-run",
        "--allow-env",
        "npm:typescript/tsc",
        "--declaration",
        "--emitDeclarationOnly",
        "--project",
        "tsconfig.json",
        "--outDir",
        distDir,
      ],
    }).output();

    console.log("Build complete!");
  } catch (error) {
    console.error("Build failed:", error);
    Deno.exit(1);
  }
}

await bundle();
