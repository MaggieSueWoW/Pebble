import { expect, test, type Page } from "@playwright/test";
import {
  collection,
  doc,
  getDoc,
  getDocs,
  setDoc,
  Timestamp,
} from "firebase/firestore";
import { setupEmulator } from "./support/emulator";
import { metadata, seedHeartbeat } from "./support/fixtures";
import { officer } from "./support/officer";
const season = "seasons/test-reports",
  night = "2026-10-14",
  other = "2026-10-15";
const path = `${season}/nightOverrides/${night}`;
const blank = { start: "", breakStart: "", breakEnd: "", end: "", notes: "" };
const detected = {
  ...blank,
  start: `${night}T19:00`,
  breakStart: `${night}T20:00`,
  breakEnd: `${night}T20:15`,
  end: `${night}T22:00`,
};
const emulator = setupEmulator();
const admin = emulator.admin;
async function timing(notes: string, version = 4) {
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), path), {
      ...blank,
      notes,
      ...metadata(version),
    });
  });
}
const notes = (page: Page) =>
  page.getByRole("textbox", { name: "Timing notes" });
const save = (page: Page) =>
  page.getByRole("button", { name: "Save timing", exact: true });
const status = (page: Page) =>
  page.getByRole("status", { name: "Timing changes" });
async function saved(page: Page) {
  await expect(status(page)).toContainText("Timing saved");
  await expect(save(page)).toBeDisabled();
}
async function open(page: Page) {
  await officer(page, emulator.environment);
  await page
    .getByRole("navigation")
    .getByRole("button", { name: "Overrides", exact: true })
    .click();
  await page
    .getByRole("combobox", { name: "Raid night", exact: true })
    .selectOption(night);
  await page.getByRole("tab", { name: "Raid timing" }).click();
  await expect(notes(page)).toHaveValue("Timing baseline");
}
// Deliberately use a non-Pacific browser timezone: values must remain Pacific wall times.
test.use({ timezoneId: "Asia/Tokyo" });
test.beforeEach(async () => {
  await timing("Timing baseline");
  await admin(async (ctx) => {
    const db = ctx.firestore();
    await setDoc(doc(db, `${season}/nightOverrides/${other}`), {
      ...blank,
      notes: "Other night",
      ...metadata(6),
    });
    await setDoc(doc(db, `${season}/reports/AAAAAAAAAAAAAAAA`), {
      enabled: true,
      notes: "Report baseline",
      ...metadata(7),
    });
    await setDoc(doc(db, `${season}/players/player-a`), {
      name: "Example Player",
      className: "Mage",
      role: "Damage",
      active: true,
      joinDate: "",
      leaveDate: "",
      ...metadata(1),
    });
    await setDoc(doc(db, `${season}/availabilityOverrides/${night}_player-a`), {
      nightId: night,
      playerId: "player-a",
      pre: { mode: "available" },
      post: { mode: "automatic" },
      notes: "Availability baseline",
      ...metadata(3),
    });
    await setDoc(doc(db, `${season}/results/fixture`), {
      complete: true,
      reportCount: 1,
      completedAt: Timestamp.now(),
    });
    await setDoc(doc(db, `${season}/results/fixture/nights/${night}`), {
      night,
      duration: 180,
      players: [],
      reports: 1,
      timing: detected,
    });
    await setDoc(doc(db, `${season}/results/fixture/nights/${other}`), {
      night: other,
      duration: 180,
      players: [],
      reports: 1,
      timing: blank,
    });
    await setDoc(doc(db, "publicSeasons/test-reports"), {
      latestRunId: "fixture",
    });
    await seedHeartbeat(db);
  });
});
test.afterEach(async () => {
  await admin(async (ctx) => {
    const db = ctx.firestore();
    expect(
      (await getDoc(doc(db, `${season}/reports/AAAAAAAAAAAAAAAA`))).data()
        ?.version,
    ).toBe(7);
    expect(
      (await getDoc(doc(db, `${season}/nightOverrides/${other}`))).data()
        ?.version,
    ).toBe(6);
    const availability = (
      await getDoc(doc(db, `${season}/availabilityOverrides/${night}_player-a`))
    ).data()!;
    expect(availability.version).toBe(3);
    expect(availability.notes).toBe("Availability baseline");
    expect((await getDocs(collection(db, `${season}/runs`))).size).toBe(0);
  });
});
test("validates timing and preserves blank/detected semantics through save, cancel and reset", async ({
  page,
}) => {
  await open(page);
  await expect(page.locator("#timing-start-help")).toContainText("7:00 PM");
  await expect(page.getByLabel("Mythic start", { exact: true })).toHaveValue(
    "",
  );
  await page.getByLabel("Break start", { exact: true }).fill(`${night}T20:05`);
  await expect(page.getByRole("alert")).toContainText("both break boundaries");
  await expect(save(page)).toBeDisabled();
  await page.getByLabel("Break end", { exact: true }).fill(`${night}T20:30`);
  await page.getByLabel("Mythic start", { exact: true }).fill(`${night}T20:10`);
  await expect(page.getByRole("alert")).toContainText("Timing must follow");
  await page.getByLabel("Mythic start", { exact: true }).fill(`${night}T19:05`);
  await page.getByLabel("Mythic end", { exact: true }).fill(`${other}T08:00`);
  await expect(page.getByRole("alert")).toContainText("12 hours");
  await page.getByLabel("Mythic end", { exact: true }).fill(`${night}T22:15`);
  await page.getByLabel("Break start", { exact: true }).fill("");
  await page.getByLabel("Break end", { exact: true }).fill("");
  await notes(page).fill("Saved timing");
  await save(page).click();
  await saved(page);
  await admin(async (ctx) => {
    const value = (await getDoc(doc(ctx.firestore(), path))).data()!;
    expect(value.start).toBe(`${night}T19:05`);
    expect(value.breakStart).toBe("");
    expect(value.breakEnd).toBe("");
    expect(value.end).toBe(`${night}T22:15`);
    expect(value.version).toBe(5);
  });
  await notes(page).fill("Discard this");
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  await expect(notes(page)).toHaveValue("Saved timing");
  await page.getByRole("button", { name: "Reset to automatic" }).click();
  await expect(
    page.getByRole("button", { name: "Run Pebble", exact: true }),
  ).toBeDisabled();
  await admin(async (ctx) => {
    expect((await getDoc(doc(ctx.firestore(), path))).data()?.version).toBe(5);
  });
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  await expect(page.getByLabel("Mythic start", { exact: true })).toHaveValue(
    `${night}T19:05`,
  );
  await page.getByRole("button", { name: "Reset to automatic" }).click();
  await save(page).click();
  await saved(page);
  await admin(async (ctx) => {
    const value = (await getDoc(doc(ctx.firestore(), path))).data()!;
    for (const key of Object.keys(blank)) expect(value[key]).toBe("");
    expect(value.version).toBe(6);
  });
});
test("holds saves, retains newer fields and guards night/tab/page navigation", async ({
  page,
}) => {
  await open(page);
  let release!: () => void, intercept!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const requested = new Promise<void>((resolve) => {
    intercept = resolve;
  });
  await page.route(/\/documents:commit(?:\?|$)/, async (route) => {
    intercept();
    await gate;
    await route.continue();
  });
  await notes(page).fill("Submitted");
  await save(page).click();
  await requested;
  await expect(status(page)).toContainText("Saving timing");
  await expect(save(page)).toBeDisabled();
  await expect(
    page.getByRole("button", { name: "Cancel", exact: true }),
  ).toBeDisabled();
  await expect(
    page.getByRole("button", { name: "Reset to automatic" }),
  ).toBeDisabled();
  await notes(page).fill("Newer notes");
  await page.getByLabel("Mythic end", { exact: true }).fill(`${night}T22:25`);
  await page
    .getByRole("combobox", { name: "Raid night", exact: true })
    .selectOption(other);
  await page.getByRole("tab", { name: "Player availability" }).click();
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /^Reports/ })
    .click();
  await expect(
    page.getByRole("region", { name: "Raid timing editor" }),
  ).toBeVisible();
  await expect(
    page.getByRole("combobox", { name: "Raid night", exact: true }),
  ).toHaveValue(night);
  await expect(page.getByRole("dialog")).not.toBeVisible();
  await expect(
    page.getByRole("button", { name: "Run Pebble", exact: true }),
  ).toBeDisabled();
  release();
  await expect(status(page)).toContainText("Timing saved");
  await expect(notes(page)).toHaveValue("Newer notes");
  await expect(save(page)).toBeEnabled();
  await page.unroute(/\/documents:commit(?:\?|$)/);
  await save(page).click();
  await saved(page);
  await admin(async (ctx) => {
    const value = (await getDoc(doc(ctx.firestore(), path))).data()!;
    expect(value.version).toBe(6);
    expect(value.notes).toBe("Newer notes");
    expect(value.end).toBe(`${night}T22:25`);
  });
});
test("retains conflicting or denied drafts, refreshes clean data, and retries", async ({
  page,
}) => {
  await open(page);
  await notes(page).fill("Local draft");
  await timing("Concurrent timing", 5);
  await save(page).click();
  await expect(page.getByRole("alert")).toContainText(
    "Another officer changed",
  );
  await expect(notes(page)).toHaveValue("Local draft");
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  await expect(notes(page)).toHaveValue("Concurrent timing");
  await timing("Pristine update", 6);
  await expect(notes(page)).toHaveValue("Pristine update");
  await expect(save(page)).toBeDisabled();
  await notes(page).fill("Retry timing");
  await page.route(/\/documents:commit(?:\?|$)/, (route) =>
    route.fulfill({
      status: 403,
      contentType: "application/json",
      body: JSON.stringify({
        error: {
          code: 403,
          status: "PERMISSION_DENIED",
          message: "Test timing save denied",
        },
      }),
    }),
  );
  await save(page).click();
  await expect(page.getByRole("alert")).toContainText(/denied|permissions/i);
  await expect(notes(page)).toHaveValue("Retry timing");
  await page.unroute(/\/documents:commit(?:\?|$)/);
  await save(page).click();
  await saved(page);
  await admin(async (ctx) => {
    const value = (await getDoc(doc(ctx.firestore(), path))).data()!;
    expect(value.version).toBe(7);
    expect(value.notes).toBe("Retry timing");
  });
});
test("handles unknown detection, Pacific DST validation, and keyboard draft protection on narrow screens", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await open(page);
  await page
    .getByRole("combobox", { name: "Raid night", exact: true })
    .selectOption(other);
  await expect(
    page.getByText(/No detected timing for this night yet/),
  ).toBeVisible();
  await page
    .getByLabel("Mythic start", { exact: true })
    .fill("2026-11-01T01:30");
  await expect(page.getByRole("alert")).toContainText("ambiguous");
  await expect(
    page.getByLabel("Mythic start", { exact: true }),
  ).toHaveAttribute("aria-invalid", "true");
  await page.getByLabel("Mythic start", { exact: true }).fill("");
  await notes(page).focus();
  await page.keyboard.press("ControlOrMeta+A");
  await page.keyboard.type("Keyboard timing draft");
  await page.keyboard.press("Tab");
  await expect(
    page.getByRole("button", { name: "Reset to automatic" }),
  ).toBeFocused();
  for (const leave of [
    () =>
      page
        .getByRole("combobox", { name: "Raid night", exact: true })
        .selectOption(night),
    () => page.getByRole("tab", { name: "Player availability" }).click(),
    () =>
      page
        .getByRole("navigation")
        .getByRole("button", { name: /^Results/ })
        .click(),
  ]) {
    await leave();
    await expect(page.getByRole("dialog")).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Keep editing" }),
    ).toBeFocused();
    await page.keyboard.press("Escape");
    await expect(notes(page)).toHaveValue("Keyboard timing draft");
  }
  await page
    .getByRole("combobox", { name: "Raid night", exact: true })
    .selectOption(night);
  await page.getByRole("button", { name: "Discard edits" }).click();
  await expect(notes(page)).toHaveValue("Timing baseline");
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});
test("keeps timing edits working in standalone sample mode", async ({
  page,
}) => {
  await page.goto("/?demo=1");
  await page
    .getByRole("navigation")
    .getByRole("button", { name: "Overrides", exact: true })
    .click();
  await page.getByRole("tab", { name: "Raid timing" }).click();
  await notes(page).fill("Sample timing notes");
  await save(page).click();
  await saved(page);
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /^Results/ })
    .click();
  await expect(page.getByRole("dialog")).not.toBeVisible();
  await page
    .getByRole("navigation")
    .getByRole("button", { name: "Overrides", exact: true })
    .click();
  await expect(notes(page)).toHaveValue("Sample timing notes");
});

test("manually activates keyboard-focused tabs and preserves dialog focus and drafts", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await open(page);
  const timing = page.getByRole("tab", { name: "Raid timing" });
  const availability = page.getByRole("tab", { name: "Player availability" });
  const panel = page.getByRole("tabpanel", { name: "Raid timing" });
  await timing.focus();
  await page.keyboard.press("ArrowRight");
  await expect(availability).toBeFocused();
  await expect(panel).toBeVisible();
  await expect(availability).toHaveAttribute("aria-selected", "false");
  await expect(availability).toHaveAttribute("tabindex", "0");
  await expect(timing).toHaveAttribute("tabindex", "-1");
  await page.keyboard.press("End");
  await expect(timing).toBeFocused();
  await page.keyboard.press("Home");
  await expect(availability).toBeFocused();
  await page.keyboard.press("ArrowLeft");
  await expect(timing).toBeFocused();
  await notes(page).fill("Retained keyboard draft");
  await timing.focus();
  await page.keyboard.press("ArrowLeft");
  await expect(availability).toBeFocused();
  await expect(page.getByRole("dialog")).not.toBeVisible();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("dialog")).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(availability).toBeFocused();
  await expect(panel).toBeVisible();
  await expect(notes(page)).toHaveValue("Retained keyboard draft");
  await page.keyboard.press("Space");
  await page.getByRole("button", { name: "Discard edits" }).click();
  await expect(availability).toBeFocused();
  await expect(
    page.getByRole("tabpanel", { name: "Player availability" }),
  ).toBeVisible();
  await expect(availability).toHaveAttribute("aria-selected", "true");
  await expect(availability).toHaveAttribute("aria-controls", "override-panel");
  await expect(page.getByRole("tabpanel")).toHaveAttribute(
    "aria-labelledby",
    (await availability.getAttribute("id")) as string,
  );
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});

test("keeps the current panel during a save and activates the focused tab after acknowledgement", async ({
  page,
}) => {
  await open(page);
  await notes(page).fill("Saved before switching tabs");
  let release!: () => void, intercept!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const requested = new Promise<void>((resolve) => {
    intercept = resolve;
  });
  await page.route(/\/documents:commit(?:\?|$)/, async (route) => {
    intercept();
    await gate;
    await route.continue();
  });
  await save(page).click();
  await requested;
  try {
    await page.getByRole("tab", { name: "Raid timing" }).focus();
    await page.keyboard.press("ArrowLeft");
    const availability = page.getByRole("tab", { name: "Player availability" });
    await expect(availability).toBeFocused();
    await page.keyboard.press("Enter");
    await expect(
      page.getByText("Wait for the save to finish.", { exact: true }),
    ).toBeVisible();
    await expect(
      page.getByRole("tabpanel", { name: "Raid timing" }),
    ).toBeVisible();
    await expect(page.getByRole("dialog")).not.toBeVisible();
    release();
    await saved(page);
    await expect(availability).toBeFocused();
    await page.keyboard.press("Enter");
    await expect(
      page.getByRole("tabpanel", { name: "Player availability" }),
    ).toBeVisible();
  } finally {
    release();
  }
});
