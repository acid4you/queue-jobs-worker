import { defineConfig } from "tsup";
import { copyFile, mkdir } from "node:fs/promises";
import { join } from "node:path";

const luaScripts = [
  "save-job",
  "update-job",
  "remove-job",
  "clear-queue",
  "list-jobs",
  "claim-job",
];

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["esm"],
  dts: true,
  clean: true,
  outDir: "dist",
  async onSuccess() {
    const destDir = join("dist", "scripts");
    await mkdir(destDir, { recursive: true });

    await Promise.all(
      luaScripts.map((name) =>
        copyFile(
          join("src", "scripts", `${name}.lua`),
          join(destDir, `${name}.lua`),
        ),
      ),
    );

    console.log(`[tsup] Copied ${luaScripts.length} Lua scripts → dist/scripts/`);
  },
});
