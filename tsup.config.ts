import { defineConfig } from "tsup";
import fs from "node:fs";
import path from "node:path";

function copyLuaFiles(srcDir: string, distDir: string) {
  if (!fs.existsSync(srcDir)) return;
  const entries = fs.readdirSync(srcDir, { withFileTypes: true });
  for (const entry of entries) {
    const srcPath = path.join(srcDir, entry.name);
    const distPath = path.join(distDir, entry.name);
    if (entry.isDirectory()) {
      copyLuaFiles(srcPath, distPath);
    } else if (entry.isFile() && entry.name.endsWith(".lua")) {
      fs.mkdirSync(distDir, { recursive: true });
      fs.copyFileSync(srcPath, distPath);
    }
  }
}

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["cjs", "esm"],
  dts: false, // declarations generated separately via tsc (see build script)
  sourcemap: true,
  clean: true,
  splitting: false,
  treeshake: true,
  outDir: "dist",
  target: "node18",
  loader: { ".lua": "text" }, // embed Lua scripts as inline strings in the bundle
  async onSuccess() {
    copyLuaFiles("src", "dist");
  },
});


