import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  // Release config supplies the public allowlist; local dotenv files stay local.
  envDir: process.env.PEBBLE_WEB_RELEASE_BUILD === "1" ? false : undefined,
});
