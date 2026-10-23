import { expect, test } from "@playwright/test";
import {
  collection,
  deleteDoc,
  doc,
  getDoc,
  getDocs,
  setDoc,
  Timestamp,
} from "firebase/firestore";
import { setupEmulator } from "./support/emulator";
import { seedHeartbeat } from "./support/fixtures";
import { officer } from "./support/officer";
const season = "seasons/test-reports";
const a = "AAAAAAAAAAAAAAAA",
  b = "BBBBBBBBBBBBBBBB",
  c = "CCCCCCCCCCCCCCCC",
  d = "DDDDDDDDDDDDDDDD";
const emulator = setupEmulator();
let expectedRunIds: string[] = [];
const admin = emulator.admin;
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
test.beforeEach(async () => {
  expectedRunIds = [];
  await seedReport(a, true);
  await seedReport(b, false);
  await admin(async (ctx) => {
    await seedHeartbeat(ctx.firestore());
  });
});
test.afterEach(async () => {
  await admin(async (ctx) => {
    expect(
      (await getDocs(collection(ctx.firestore(), `${season}/runs`))).docs
        .map((d) => d.id)
        .sort(),
    ).toEqual(expectedRunIds);
  });
});
test("validates a single report, adds once, re-enables, and saves/cancels selections", async ({
  page,
}) => {
  await officer(page, emulator.environment);
  const input = page.getByRole("textbox", { name: "Warcraft Logs reports" });
  await input.fill("invalid");
  await expect(input).toHaveAttribute("aria-invalid", "true");
  await expect(
    page.getByRole("button", { name: "Add report", exact: true }),
  ).toBeDisabled();
  await input.fill(`https://www.warcraftlogs.com/reports/${c}`);
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
  await officer(page, emulator.environment);
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
    page.getByRole("textbox", { name: "Warcraft Logs reports" }),
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
  await officer(page, emulator.environment);
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
  await officer(page, emulator.environment);
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

test("explains an empty report list and keeps Add a report available", async ({
  page,
}) => {
  await admin(async (ctx) => {
    await deleteDoc(doc(ctx.firestore(), `${season}/reports/${a}`));
    await deleteDoc(doc(ctx.firestore(), `${season}/reports/${b}`));
  });
  await officer(page, emulator.environment);
  await expect(
    page.getByText(/No reports yet. Paste a Warcraft Logs link/),
  ).toBeVisible();
  const input = page.getByRole("textbox", { name: "Warcraft Logs reports" });
  await input.focus();
  await expect(input).toBeFocused();
  await expect(page.getByRole("switch")).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Add report", exact: true }),
  ).toBeDisabled();
});

test("shows recorded report details and refreshes metadata without changing drafts or inputs", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await seedReport(c, true);
  expectedRunIds = ["report-metadata"];
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), `${season}/runs/report-metadata`), {
      status: "succeeded",
      requestedAt: Timestamp.fromDate(new Date("2026-10-16T03:00:00Z")),
      reportSummaries: {
        [a]: {
          title: "  ",
          night: "2026-10-15",
          checkedAt: Timestamp.fromDate(new Date("2026-10-16T02:05:00Z")),
        },
        [b]: { title: "Example older report", night: "2026-10-14" },
      },
    });
  });
  await officer(page, emulator.environment);
  const row = (code: string) =>
    page
      .getByRole("row")
      .filter({ has: page.getByRole("switch", { name: `Include ${code}` }) });
  const firstRow = row(a);
  const link = firstRow.getByRole("link", { name: a, exact: true });
  await expect(link).toHaveAttribute(
    "href",
    `https://www.warcraftlogs.com/reports/${a}`,
  );
  await expect(firstRow.locator("time")).toHaveText(
    "Oct 15, 2026, 7:05 PM PDT",
  );
  await expect(firstRow.locator("time")).toHaveAttribute(
    "datetime",
    "2026-10-16T02:05:00.000Z",
  );
  await expect(row(b)).toContainText("Last checked unavailable");
  await expect(row(b).getByRole("link")).toHaveText("Example older report");
  await expect(row(c)).toContainText("Metadata appears after the next run");
  await expect(row(c)).toContainText("Last checked unavailable");
  await link.focus();
  await page.keyboard.press("Tab");
  await expect(firstRow.getByRole("switch")).toBeFocused();
  await page.keyboard.press("Space");
  await admin(async (ctx) => {
    await setDoc(
      doc(ctx.firestore(), `${season}/runs/report-metadata`),
      {
        reportSummaries: {
          [a]: {
            title: "Example updated raid",
            night: "2026-10-15",
            checkedAt: Timestamp.fromDate(new Date("2026-10-17T02:05:00Z")),
          },
        },
      },
      { merge: true },
    );
  });
  await expect(firstRow.getByRole("link")).toHaveText("Example updated raid");
  await expect(firstRow.locator("time")).toHaveText(
    "Oct 16, 2026, 7:05 PM PDT",
  );
  await expect(firstRow.getByRole("switch")).toHaveAttribute(
    "aria-checked",
    "false",
  );
  await expect(page.getByText(/1 unsaved selection change/)).toBeVisible();
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  await expect(firstRow.getByRole("switch")).toHaveAttribute(
    "aria-checked",
    "true",
  );
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  await admin(async (ctx) => {
    for (const [code, enabled] of [
      [a, true],
      [b, false],
      [c, true],
    ] as const) {
      const saved = (
        await getDoc(doc(ctx.firestore(), `${season}/reports/${code}`))
      ).data()!;
      expect(saved.version).toBe(1);
      expect(saved.enabled).toBe(enabled);
    }
  });
});

test("deletes a report without excluding it, preserves other inputs, and allows adding it again", async ({
  page,
}) => {
  await officer(page, emulator.environment);
  const remove = page.getByRole("button", {
    name: `Delete report ${a}`,
    exact: true,
  });
  await expect(remove).toBeVisible();
  await remove.click();
  await expect(
    page.getByRole("status", { name: "Report changes" }),
  ).toContainText("Report deleted");
  await expect(remove).toHaveCount(0);
  await expect(
    page.getByRole("switch", { name: `Include ${b}` }),
  ).toHaveAttribute("aria-checked", "false");
  await admin(async (ctx) => {
    expect(
      (await getDoc(doc(ctx.firestore(), `${season}/reports/${a}`))).exists(),
    ).toBe(false);
    expect(
      (await getDoc(doc(ctx.firestore(), `${season}/reports/${b}`))).data()
        ?.version,
    ).toBe(1);
  });
  await page.getByRole("textbox", { name: "Warcraft Logs reports" }).fill(a);
  await page.getByRole("button", { name: "Add report", exact: true }).click();
  await expect(remove).toBeVisible();
  await expect(
    page.getByRole("switch", { name: `Include ${a}` }),
  ).toHaveAttribute("aria-checked", "true");
});
