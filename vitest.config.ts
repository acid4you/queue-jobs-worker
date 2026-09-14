import { defineConfig, type Plugin } from "vitest/config";

/** Load *.lua files as plain text strings (mirroring tsup's `loader: { ".lua": "text" }`). */
function rawLuaPlugin(): Plugin {
  return {
    name: "raw-lua",
    transform(code, id) {
      if (!id.endsWith(".lua")) return null;
      const json = JSON.stringify(code);
      return { code: `export default ${json};`, map: null };
    },
  };
}

export default defineConfig({
  plugins: [rawLuaPlugin()],
  test: {
    globals: true,
    environment: "node",
    include: ["tests/**/*.test.ts"],
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "html"],
      exclude: ["node_modules", "dist", "tests"],
    },
  },
});

