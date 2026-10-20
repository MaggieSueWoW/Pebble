import { test, expect } from "@playwright/test";
import {
  initializeTestEnvironment,
  type RulesTestEnvironment,
} from "@firebase/rules-unit-testing";
import { doc, setDoc } from "firebase/firestore";
import { readFileSync } from "node:fs";
import { signIn } from "./officer";

let env: RulesTestEnvironment;
const admin = (
  fn: Parameters<RulesTestEnvironment["withSecurityRulesDisabled"]>[0],
) => env.withSecurityRulesDisabled(fn);
test.beforeAll(async () => {
  env = await initializeTestEnvironment({
    projectId: "demo-pebble-reports",
    firestore: {
      host: "127.0.0.1",
      port: 8180,
      rules: readFileSync(
        new URL("../../../firebase/firestore.rules", import.meta.url),
        "utf8",
      ),
    },
  });
});
test.beforeEach(async () => {
  await env.clearFirestore();
});
test.afterAll(async () => {
  await env?.cleanup();
});

test("Google emulator onboarding, refresh, revocation during editing, regrant and sign-out", async ({
  page,
}) => {
  const uid = await signIn(page);
  await expect(page.getByRole("button", { name: /^Reports/ })).toHaveCount(0);
  await page.reload();
  await expect(
    page.getByRole("region", { name: "Officer access not enabled" }),
  ).toBeVisible();
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), `officers/${uid}`), { enabled: true });
  });
  const reports = () =>
    page.getByRole("navigation").getByRole("button", { name: /^Reports/ });
  await reports().click();
  const draft = () =>
    page.getByRole("textbox", { name: "Warcraft Logs reports" });
  await expect(draft()).toBeVisible();
  await page.reload();
  await reports().click();
  await expect(draft()).toHaveValue("");
  await draft().fill("AAAAAAAAAAAAAAAA");
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), `officers/${uid}`), { enabled: false });
  });
  await expect(
    page.getByRole("region", { name: "Officer access not enabled" }),
  ).toBeVisible();
  await expect(draft()).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Run Pebble", exact: true }),
  ).toHaveCount(0);
  await expect(page.getByRole("button", { name: /Run history/ })).toHaveCount(
    0,
  );
  await page.reload();
  await expect(
    page.getByRole("region", { name: "Officer access not enabled" }),
  ).toBeVisible();
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), `officers/${uid}`), { enabled: true });
  });
  await reports().click();
  await expect(draft()).toHaveValue("");
  await page.getByRole("button", { name: "Sign out", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "Officer sign-in", exact: true }),
  ).toBeVisible();
  await expect(draft()).toHaveCount(0);
});

test("blocked Google popup gives actionable recovery and allows retry", async ({
  page,
}) => {
  await page.addInitScript(() => {
    window.open = () => null;
  });
  await page.goto("/");
  const signIn = page.getByRole("button", {
    name: "Officer sign-in",
    exact: true,
  });
  await signIn.click();
  await expect(page.getByRole("alert")).toContainText(
    "browser blocked Google sign-in",
  );
  await expect(signIn).toBeEnabled();
});

test("cancelled Google popup returns to public view with retry", async ({
  page,
}) => {
  await page.goto("/");
  const popupPromise = page.waitForEvent("popup");
  const signIn = page.getByRole("button", {
    name: "Officer sign-in",
    exact: true,
  });
  await signIn.click();
  const popup = await popupPromise;
  await popup.waitForLoadState("load");
  await popup.close();
  // Firebase allows eight seconds for an in-flight auth event after closure.
  await expect(page.getByRole("alert")).toContainText("sign-in was cancelled", {
    timeout: 15000,
  });
  await expect(signIn).toBeEnabled();
  await expect(page.getByRole("button", { name: /^Reports/ })).toHaveCount(0);
});
