import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: false,
    environment: "node",
    include: ["tests/**/*.test.ts"],
    // Each test file runs in its own worker so global state (default client)
    // never bleeds between files.
    pool: "forks",
    reporters: ["verbose"],
    // Retry/backoff/delay tests need more time than the default 5 s.
    testTimeout: 30_000,
    hookTimeout: 15_000,
  },
});
