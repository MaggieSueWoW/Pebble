import { test, expect, type Page } from "@playwright/test";
import {
  initializeTestEnvironment,
  type RulesTestEnvironment,
} from "@firebase/rules-unit-testing";
import {
  collection,
  doc,
  getDoc,
  getDocs,
  setDoc,
  Timestamp,
} from "firebase/firestore";
import { readFileSync } from "node:fs";
import { officer } from "./officer";
import { defaultCalculation } from "../src/calculation";
let env: RulesTestEnvironment;
const season = "seasons/test-reports";
const admin = (
  fn: Parameters<RulesTestEnvironment["withSecurityRulesDisabled"]>[0],
) => env.withSecurityRulesDisabled(fn);
const settings = (extension: number, version: number) => ({
  ...defaultCalculation(),
  postExtensionMinutes: extension,
  version,
  updatedBy: "fixture",
  updatedAt: Timestamp.now(),
});
const save = (page: Page) =>
  page.getByRole("button", { name: "Save calculation settings", exact: true });
async function open(page: Page) {
  await officer(page, env);
  await page
    .getByRole("navigation")
    .getByRole("button", { name: "Settings", exact: true })
    .click();
  await expect(
    page.getByRole("heading", {
      name: "Calculation settings",
      exact: true,
      level: 2,
    }),
  ).toBeVisible();
}
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
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), "bot/status"), {
      lastHeartbeatAt: Timestamp.now(),
    });
    await setDoc(doc(ctx.firestore(), `${season}/reports/AAAAAAAAAAAAAAAA`), {
      enabled: true,
      version: 1,
      updatedBy: "fixture",
      updatedAt: Timestamp.now(),
    });
  });
});
test.afterEach(async () => {
  await admin(async (ctx) => {
    expect(
      (await getDocs(collection(ctx.firestore(), `${season}/runs`))).empty,
    ).toBe(true);
    expect(
      (
        await getDoc(doc(ctx.firestore(), `${season}/reports/AAAAAAAAAAAAAAAA`))
      ).data()?.version,
    ).toBe(1);
  });
});
test.afterAll(async () => {
  await env.cleanup();
});
test("creates default settings with validation, guarded navigation and no recomputation", async ({
  page,
}) => {
  await open(page);
  await expect(
    page.getByLabel("Break window start", { exact: true }),
  ).toHaveValue("20:50");
  await expect(save(page)).toBeDisabled();
  await page.getByLabel("Minimum break minutes").fill("0");
  await expect(page.getByRole("alert")).toContainText("whole minutes");
  await expect(save(page)).toBeDisabled();
  await page.getByLabel("Minimum break minutes").fill("12");
  await expect(
    page.getByRole("button", { name: "Run Pebble", exact: true }),
  ).toHaveCount(0);
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /^Reports/ })
    .click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await page.keyboard.press("Escape");
  await page.getByLabel("Default mythic start").fill("19:35");
  await page.getByLabel("Post extension minutes").fill("2.5");
  await save(page).click();
  await expect(
    page.getByRole("status", { name: "Calculation settings changes" }),
  ).toContainText("Calculation settings saved");
  await admin(async (ctx) => {
    expect(
      (
        await getDoc(doc(ctx.firestore(), `${season}/settings/calculation`))
      ).data(),
    ).toMatchObject({
      ...defaultCalculation(),
      minBreakMinutes: 12,
      defaultMythicStart: "19:35",
      postExtensionMinutes: 2.5,
      version: 1,
    });
  });
  await page.setViewportSize({ width: 390, height: 844 });
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});
test("preserves a stale draft on conflict and cancels to the latest remote settings", async ({
  page,
}) => {
  await admin(async (ctx) => {
    await setDoc(
      doc(ctx.firestore(), `${season}/settings/calculation`),
      settings(6, 1),
    );
  });
  await open(page);
  await expect(page.getByLabel("Post extension minutes")).toHaveValue("6");
  await page.getByLabel("Post extension minutes").fill("8");
  await admin(async (ctx) => {
    await setDoc(
      doc(ctx.firestore(), `${season}/settings/calculation`),
      settings(10, 2),
    );
  });
  await save(page).click();
  await expect(
    page
      .getByRole("alert")
      .filter({ hasText: "Another officer changed calculation settings" })
      .first(),
  ).toBeVisible();
  await expect(page.getByLabel("Post extension minutes")).toHaveValue("8");
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  await expect(page.getByLabel("Post extension minutes")).toHaveValue("10");
  await page.getByLabel("Post extension minutes").fill("11");
  await save(page).click();
  await expect(
    page.getByRole("status", { name: "Calculation settings changes" }),
  ).toContainText("Calculation settings saved");
  await admin(async (ctx) => {
    expect(
      (
        await getDoc(doc(ctx.firestore(), `${season}/settings/calculation`))
      ).data(),
    ).toMatchObject({ postExtensionMinutes: 11, version: 3 });
  });
});
