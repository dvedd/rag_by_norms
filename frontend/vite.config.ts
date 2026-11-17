import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ mode }) => {
  const env = { ...loadEnv(mode, process.cwd(), ""), ...process.env };

  return {
    plugins: [react()],
    server: {
      port: 5173,
      proxy: {
        "/api/retriever": {
          target: env.RETRIEVER_API_URL || "http://127.0.0.1:9123",
          rewrite: (path) => path.replace(/^\/api\/retriever/, ""),
          proxyTimeout: 300_000,
        },
        "/api/user": {
          target: env.USER_API_URL || "http://127.0.0.1:9130",
          rewrite: (path) => path.replace(/^\/api\/user/, ""),
        },
      },
    },
  };
});
