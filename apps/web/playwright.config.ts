import { defineConfig } from "@playwright/test";
export default defineConfig({
  testDir: "./e2e",
  workers: 1,
  timeout: 30000,
  use: {
    baseURL: "http://127.0.0.1:5174",
    channel: "chrome",
    trace: "retain-on-failure",
  },
  webServer: {
    command: "npm run dev -- --port 5174 --strictPort",
    url: "http://127.0.0.1:5174",
    reuseExistingServer: false,
    env: {
      VITE_FIREBASE_PROJECT_ID: "demo-pebble-reports",
      VITE_FIREBASE_API_KEY: "demo-api-key",
      VITE_FIREBASE_APP_ID: "demo-app",
      VITE_FIREBASE_AUTH_DOMAIN: "demo-pebble-reports.firebaseapp.com",
      VITE_FIREBASE_USE_EMULATORS: "true",
      VITE_FIREBASE_AUTH_EMULATOR_URL: "http://127.0.0.1:9199",
      VITE_FIRESTORE_EMULATOR_HOST: "127.0.0.1",
      VITE_FIRESTORE_EMULATOR_PORT: "8180",
      VITE_FUNCTIONS_EMULATOR_PORT: "5101",
      VITE_PEBBLE_SEASON_ID: "test-reports",
      VITE_PEBBLE_SEASON_LABEL: "Browser acceptance",
    },
  },
});
