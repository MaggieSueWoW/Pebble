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
const projectId = "demo-pebble-reports",
  season = "seasons/test-reports";
const a = "AAAAAAAAAAAAAAAA",
  b = "BBBBBBBBBBBBBBBB",
  c = "CCCCCCCCCCCCCCCC",
  d = "DDDDDDDDDDDDDDDD";
let env: RulesTestEnvironment;
const admin = (
  fn: Parameters<RulesTestEnvironment["withSecurityRulesDisabled"]>[0],
) => env.withSecurityRulesDisabled(fn);
const seedReport = (code: string, enabled: boolean, version = 1, notes = "") =>
  admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), `${season}/reports/${code}`), {
      enabled,
      version,
      notes,
      updatedBy: "fixture",
      updatedAt: Timestamp.now(),
    });
  });
async function officer(page: Page) {
  await page.goto("/");
  const popupPromise = page.waitForEvent("popup");
  await page.getByRole("button", { name: "Officer sign-in" }).click();
  const popup = await popupPromise;
  await popup.waitForLoadState("load");
  await popup.getByRole("button", { name: "Add new account" }).click();
  await popup
    .locator("#email-input")
    .fill(`officer-${Date.now()}@example.test`);
  await popup.locator("#display-name-input").fill("Example Officer");
  await popup.locator("#sign-in").click();
  const notice = page.getByText(/This account does not have officer access/);
  await expect(notice).toBeVisible();
  const uid = (await notice.innerText()).split("Firebase UID:")[1].trim();
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), `officers/${uid}`), { enabled: true });
  });
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /^Reports/ })
    .click();
  await expect(
    page.getByRole("heading", { name: "Add a report" }),
  ).toBeVisible();
}
test.beforeAll(async () => {
  if (!projectId.startsWith("demo-"))
    throw new Error("Browser tests require a demo project");
  env = await initializeTestEnvironment({
    projectId,
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
  await seedReport(a, true);
  await seedReport(b, false);
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), "bot/status"), {
      lastHeartbeatAt: Timestamp.now(),
    });
  });
});
test.afterEach(async () => {
  await admin(async (ctx) => {
    expect(
      (await getDocs(collection(ctx.firestore(), `${season}/runs`))).size,
    ).toBe(0);
  });
});
test.afterAll(async () => {
  await env?.cleanup();
});
test("validates batches, adds once, re-enables, and saves/cancels selections", async ({
  page,
}) => {
  await officer(page);
  const input = page.getByRole("textbox", { name: "Warcraft Logs reports" });
  await input.fill(`${c}\ninvalid`);
  await expect(input).toHaveAttribute("aria-invalid", "true");
  await expect(
    page.getByRole("button", { name: "Add report", exact: true }),
  ).toBeDisabled();
  await input.fill(`${c}\nhttps://www.warcraftlogs.com/reports/${c}`);
  await expect(
    page.getByText("Repeated in this list · will be skipped"),
  ).toBeVisible();
  await page.getByRole("button", { name: "Add report", exact: true }).click();
  await expect(input).toHaveValue("");
  await expect(
    page.getByRole("switch", { name: `Include ${c}` }),
  ).toBeVisible();
  await input.fill(b);
  await page.getByRole("button", { name: "Re-enable", exact: true }).click();
  await expect(input).toHaveValue("");
  await expect(
    page.getByRole("switch", { name: `Include ${b}` }),
  ).toHaveAttribute("aria-checked", "true");
  await page.getByRole("switch", { name: `Include ${a}` }).click();
  await expect(
    page.getByRole("button", { name: "Run Pebble", exact: true }),
  ).toBeDisabled();
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  await expect(
    page.getByRole("switch", { name: `Include ${a}` }),
  ).toHaveAttribute("aria-checked", "true");
  await page.getByRole("switch", { name: `Include ${a}` }).click();
  await page
    .getByRole("button", { name: "Save selection", exact: true })
    .click();
  await expect(
    page.getByRole("button", { name: "Save selection", exact: true }),
  ).toHaveCount(0);
  await admin(async (ctx) => {
    const db = ctx.firestore();
    expect((await getDocs(collection(db, `${season}/reports`))).size).toBe(3);
    expect(
      (await getDoc(doc(db, `${season}/reports/${a}`))).data()?.enabled,
    ).toBe(false);
    expect(
      (await getDoc(doc(db, `${season}/reports/${b}`))).data()?.enabled,
    ).toBe(true);
  });
});
test("holds a pending save, preserves newer text, and blocks navigation/run", async ({
  page,
}) => {
  await officer(page);
  let release!: () => void;
  let intercepted!: () => void;
  const requested = new Promise<void>((resolve) => {
    intercepted = resolve;
  });
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  await page.route(/\/documents:commit(?:\?|$)/, async (route) => {
    intercepted();
    await gate;
    await route.continue();
  });
  const input = page.getByRole("textbox", { name: "Warcraft Logs reports" });
  await input.fill(c);
  await page.getByRole("button", { name: "Add report", exact: true }).click();
  await expect(
    page.getByRole("status", { name: "Report changes" }),
  ).toContainText("Saving");
  await requested;
  await input.fill(d);
  await expect(
    page.getByRole("button", { name: "Add report", exact: true }),
  ).toBeDisabled();
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /^Results/ })
    .click();
  await expect(
    page.getByRole("heading", { name: "Add a report" }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Run Pebble", exact: true }),
  ).toBeDisabled();
  release();
  await expect(
    page.getByRole("status", { name: "Report changes" }),
  ).toContainText("Reports added");
  await expect(input).toHaveValue(d);
});
test("retains failed saves and version conflicts until cancel/retry", async ({
  page,
}) => {
  await officer(page);
  await page.getByRole("switch", { name: `Include ${a}` }).click();
  await seedReport(a, true, 2, "Concurrent edit");
  await page
    .getByRole("button", { name: "Save selection", exact: true })
    .click();
  await expect(
    page
      .getByRole("alert")
      .filter({ hasText: "Another officer changed" })
      .first(),
  ).toBeVisible();
  await expect(
    page.getByRole("switch", { name: `Include ${a}` }),
  ).toHaveAttribute("aria-checked", "false");
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  await expect(
    page.getByRole("switch", { name: `Include ${a}` }),
  ).toHaveAttribute("aria-checked", "true");
  const input = page.getByRole("textbox", { name: "Warcraft Logs reports" });
  await input.fill(c);
  await page.route(/\/documents:commit(?:\?|$)/, (route) =>
    route.fulfill({
      status: 403,
      contentType: "application/json",
      body: JSON.stringify({
        error: {
          code: 403,
          status: "PERMISSION_DENIED",
          message: "Test save denied",
        },
      }),
    }),
  );
  await page.getByRole("button", { name: "Add report", exact: true }).click();
  await expect(
    page
      .getByRole("alert")
      .filter({ hasText: /Test save denied|permissions/i })
      .first(),
  ).toBeVisible();
  await expect(input).toHaveValue(c);
  await page.unroute(/\/documents:commit(?:\?|$)/);
  await page.getByRole("button", { name: "Add report", exact: true }).click();
  await expect(input).toHaveValue("");
});
test("protects drafts with a keyboard-accessible dialog at a narrow viewport", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await officer(page);
  const input = page.getByRole("textbox", { name: "Warcraft Logs reports" });
  await input.fill(c);
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /^Results/ })
    .click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Keep editing" }),
  ).toBeFocused();
  await page.keyboard.press("Escape");
  await expect(input).toHaveValue(c);
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /^Results/ })
    .click();
  await page.getByRole("button", { name: "Discard edits" }).click();
  await expect(page.getByRole("dialog")).not.toBeVisible();
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /^Reports/ })
    .click();
  await expect(input).toHaveValue("");
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});
