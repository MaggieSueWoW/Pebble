import {
  initializeTestEnvironment,
  type RulesTestEnvironment,
} from "@firebase/rules-unit-testing";
import { test } from "@playwright/test";
import { readFileSync } from "node:fs";

// Fixed demo project and loopback address: never use the developer's Firebase config.
const projectId = "demo-pebble-reports";
export const firestoreRules = readFileSync(
  new URL("../../../../firebase/firestore.rules", import.meta.url),
  "utf8",
);

async function createEnvironment(rules = firestoreRules) {
  return initializeTestEnvironment({
    projectId,
    firestore: { host: "127.0.0.1", port: 8180, rules },
  });
}

/** Register once per spec, before scenario hooks. Specs share one emulator worker. */
export function setupEmulator() {
  let environment: RulesTestEnvironment | undefined;
  const current = () => {
    if (!environment)
      throw new Error("Browser test emulator is not initialized");
    return environment;
  };

  test.beforeAll(async () => {
    environment = await createEnvironment();
  });
  test.beforeEach(async () => {
    await current().clearFirestore();
  });
  test.afterAll(async () => {
    await environment?.cleanup();
    environment = undefined;
  });

  return {
    get environment() {
      return current();
    },
    admin: (
      fn: Parameters<RulesTestEnvironment["withSecurityRulesDisabled"]>[0],
    ) => current().withSecurityRulesDisabled(fn),
  };
}

/** Replace rules for a denial scenario; callers restore them in a finally block. */
export async function setEmulatorRules(rules: string) {
  const temporary = await createEnvironment(rules);
  await temporary.cleanup();
}
