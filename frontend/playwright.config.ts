import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  testMatch: "*.spec.ts",
  fullyParallel: false,
  use: {
    baseURL: "http://127.0.0.1:5175",
    viewport: { width: 1440, height: 1000 },
    reducedMotion: "reduce",
    launchOptions: {
      executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH || undefined,
      args: ["--no-sandbox"],
    },
  },
  webServer: [
    { command: "node tests/stream-server.mjs", port: 9138 },
    { command: "npm run dev -- --port 5175", port: 5175,
      env: { RETRIEVER_API_URL: "http://127.0.0.1:9138" } },
  ],
});
