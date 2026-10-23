import { expect, test, type Page } from "@playwright/test";
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
import { metadata, seedHeartbeat } from "./support/fixtures";
import { officer } from "./support/officer";
const season = "seasons/test-reports";
const first = "2026-10-14",
  second = "2026-10-15";
const path = `${season}/availabilityOverrides/${first}_player-a`;
const emulator = setupEmulator();
const admin = emulator.admin;
async function override(notes: string, version = 1, pre = "automatic") {
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), path), {
      nightId: first,
      playerId: "player-a",
      pre: { mode: pre },
      post: { mode: "automatic" },
      notes,
      ...metadata(version),
    });
  });
}
async function open(page: Page) {
  await officer(page, emulator.environment);
  await page
    .getByRole("navigation")
    .getByRole("button", { name: "Overrides", exact: true })
    .click();
  await expect(
    page.getByRole("heading", { name: "Who was available?" }),
  ).toBeVisible();
  await expect(
    page.getByRole("combobox", { name: "Raid night", exact: true }),
  ).toHaveValue(/^[0-9]{4}-[0-9]{2}-[0-9]{2}$/);
  await page
    .getByRole("combobox", { name: "Raid night", exact: true })
    .selectOption(first);
  await expect(
    page.getByRole("heading", { name: "Example Alpha", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("textbox", { name: "Availability notes" }),
  ).toHaveValue("Saved Alpha");
}
const notes = (page: Page) =>
  page.getByRole("textbox", { name: "Availability notes" });
const saveButton = (page: Page) =>
  page.getByRole("button", { name: "Save adjustment", exact: true });
const status = (page: Page) =>
  page.getByRole("status", { name: "Availability changes" });
async function unchanged() {
  await admin(async (ctx) => {
    const db = ctx.firestore();
    expect(
      (await getDoc(doc(db, `${season}/reports/AAAAAAAAAAAAAAAA`))).data()
        ?.version,
    ).toBe(7);
    expect(
      (await getDoc(doc(db, `${season}/nightOverrides/${first}`))).data()
        ?.version,
    ).toBe(4);
    const otherPlayer = (
      await getDoc(doc(db, `${season}/availabilityOverrides/${first}_player-b`))
    ).data()!;
    expect(otherPlayer.version).toBe(5);
    expect(otherPlayer.notes).toBe("Other player");
    const otherNight = (
      await getDoc(
        doc(db, `${season}/availabilityOverrides/${second}_player-a`),
      )
    ).data()!;
    expect(otherNight.version).toBe(6);
    expect(otherNight.notes).toBe("Other night");
    expect((await getDocs(collection(db, `${season}/runs`))).size).toBe(0);
  });
}
test.beforeEach(async () => {
  await override("Saved Alpha");
  await admin(async (ctx) => {
    const db = ctx.firestore();
    for (const [id, name] of [
      ["player-a", "Example Alpha"],
      ["player-b", "Example Beta"],
    ]) {
      await setDoc(doc(db, `${season}/players/${id}`), {
        name,
        className: "Mage",
        role: "Damage",
        active: true,
        joinDate: "",
        leaveDate: "",
        ...metadata(1),
      });
    }
    await setDoc(doc(db, `${season}/reports/AAAAAAAAAAAAAAAA`), {
      enabled: true,
      notes: "Report baseline",
      ...metadata(7),
    });
    await setDoc(doc(db, `${season}/nightOverrides/${first}`), {
      start: "",
      breakStart: "",
      breakEnd: "",
      end: "",
      notes: "Timing baseline",
      ...metadata(4),
    });
    await setDoc(doc(db, `${season}/availabilityOverrides/${first}_player-b`), {
      nightId: first,
      playerId: "player-b",
      pre: { mode: "unavailable" },
      post: { mode: "available" },
      notes: "Other player",
      ...metadata(5),
    });
    await setDoc(
      doc(db, `${season}/availabilityOverrides/${second}_player-a`),
      {
        nightId: second,
        playerId: "player-a",
        pre: { mode: "available" },
        post: { mode: "automatic" },
        notes: "Other night",
        ...metadata(6),
      },
    );
    await setDoc(doc(db, `${season}/results/fixture`), {
      complete: true,
      reportCount: 1,
      completedAt: Timestamp.now(),
    });
    for (const night of [first, second]) {
      await setDoc(doc(db, `${season}/results/fixture/nights/${night}`), {
        night,
        duration: 180,
        players: [],
        reports: 1,
      });
    }
    await setDoc(doc(db, "publicSeasons/test-reports"), {
      latestRunId: "fixture",
    });
    await seedHeartbeat(db);
  });
});
test.afterEach(unchanged);
test("validates modes/minutes and saves, cancels, and resets only the selected override", async ({
  page,
}) => {
  await open(page);
  await page
    .getByRole("combobox", { name: "Before break", exact: true })
    .selectOption("partial");
  const minutes = page.getByRole("spinbutton", {
    name: "Before break available minutes",
  });
  for (const value of ["0", "1.5", "721"]) {
    await minutes.fill(value);
    await expect(minutes).toHaveAttribute("aria-invalid", "true");
    await expect(saveButton(page)).toBeDisabled();
    await expect(
      page.getByRole("alert").filter({ hasText: "Before break" }),
    ).toBeVisible();
  }
  await minutes.fill("60");
  await page
    .getByRole("combobox", { name: "After break", exact: true })
    .selectOption("unavailableFor");
  const absent = page.getByRole("spinbutton", {
    name: "After break unavailable minutes",
  });
  await absent.fill("721");
  await expect(
    page.getByRole("alert").filter({ hasText: "between 1 and 720" }),
  ).toBeVisible();
  await absent.fill("10");
  await notes(page).fill("Saved partial availability");
  await saveButton(page).click();
  await expect(status(page)).toContainText("Availability saved");
  await expect(saveButton(page)).toBeDisabled();
  await admin(async (ctx) => {
    const saved = (await getDoc(doc(ctx.firestore(), path))).data()!;
    expect(saved.pre).toEqual({ mode: "partial", minutes: 60 });
    expect(saved.post).toEqual({ mode: "unavailableFor", minutes: 10 });
    expect(saved.version).toBe(2);
  });
  await notes(page).fill("Discard me");
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  await expect(notes(page)).toHaveValue("Saved partial availability");
  await page.getByRole("button", { name: "Reset to automatic" }).click();
  await expect(
    page.getByRole("combobox", { name: "Before break", exact: true }),
  ).toHaveValue("automatic");
  await expect(
    page.getByRole("button", { name: "Run Pebble", exact: true }),
  ).toBeDisabled();
  await admin(async (ctx) => {
    expect((await getDoc(doc(ctx.firestore(), path))).data()?.version).toBe(2);
  });
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  await expect(minutes).toHaveValue("60");
  await page.getByRole("button", { name: "Reset to automatic" }).click();
  await saveButton(page).click();
  await expect(status(page)).toContainText("Availability saved");
  await expect(saveButton(page)).toBeDisabled();
  await expect(status(page)).toContainText("Availability saved");
  await admin(async (ctx) => {
    const saved = (await getDoc(doc(ctx.firestore(), path))).data()!;
    expect(saved.pre).toEqual({ mode: "automatic" });
    expect(saved.post).toEqual({ mode: "automatic" });
    expect(saved.notes).toBe("");
    expect(saved.version).toBe(3);
  });
});
test("preserves newer field edits through a held save and guards navigation and repeated actions", async ({
  page,
}) => {
  await open(page);
  let release!: () => void, intercepted!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const requested = new Promise<void>((resolve) => {
    intercepted = resolve;
  });
  await page.route(/\/documents:commit(?:\?|$)/, async (route) => {
    intercepted();
    await gate;
    await route.continue();
  });
  await notes(page).fill("Submitted notes");
  await saveButton(page).click();
  await requested;
  await expect(status(page)).toContainText("Saving availability");
  await expect(saveButton(page)).toBeDisabled();
  await expect(
    page.getByRole("button", { name: "Cancel", exact: true }),
  ).toBeDisabled();
  await expect(
    page.getByRole("button", { name: "Reset to automatic" }),
  ).toBeDisabled();
  await notes(page).fill("Newer notes");
  await page
    .getByRole("combobox", { name: "After break", exact: true })
    .selectOption("unavailable");
  await page.getByRole("button", { name: /Example Beta/ }).click();
  await page
    .getByRole("combobox", { name: "Raid night", exact: true })
    .selectOption(second);
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("button", { name: "Review Night", exact: true })
    .click();
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /^Reports/ })
    .click();
  await expect(
    page.getByRole("heading", { name: "Example Alpha", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("combobox", { name: "Raid night", exact: true }),
  ).toHaveValue(first);
  await expect(page.getByRole("dialog")).not.toBeVisible();
  await expect(
    page.getByRole("button", { name: "Run Pebble", exact: true }),
  ).toBeDisabled();
  release();
  await expect(status(page)).toContainText("Availability saved");
  await expect(notes(page)).toHaveValue("Newer notes");
  await expect(saveButton(page)).toBeEnabled();
  await page.unroute(/\/documents:commit(?:\?|$)/);
  await saveButton(page).click();
  await expect(status(page)).toContainText("Availability saved");
  await expect(saveButton(page)).toBeDisabled();
  await admin(async (ctx) => {
    const saved = (await getDoc(doc(ctx.firestore(), path))).data()!;
    expect(saved.version).toBe(3);
    expect(saved.notes).toBe("Newer notes");
    expect(saved.post.mode).toBe("unavailable");
  });
});
test("retains conflicts/denied writes, refreshes clean snapshots, and retries with current versions", async ({
  page,
}) => {
  await open(page);
  await notes(page).fill("Local draft");
  await override("Remote while editing", 2, "available");
  await saveButton(page).click();
  await expect(
    page.getByRole("alert").filter({ hasText: "Another officer changed" }),
  ).toBeVisible();
  await expect(notes(page)).toHaveValue("Local draft");
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  await expect(notes(page)).toHaveValue("Remote while editing");
  await expect(
    page.getByRole("combobox", { name: "Before break", exact: true }),
  ).toHaveValue("available");
  await override("Remote while pristine", 3, "available");
  await expect(notes(page)).toHaveValue("Remote while pristine");
  await expect(saveButton(page)).toBeDisabled();
  await notes(page).fill("Retry draft");
  await page.route(/\/documents:commit(?:\?|$)/, (route) =>
    route.fulfill({
      status: 403,
      contentType: "application/json",
      body: JSON.stringify({
        error: {
          code: 403,
          status: "PERMISSION_DENIED",
          message: "Test availability save denied",
        },
      }),
    }),
  );
  await saveButton(page).click();
  await expect(
    page.getByRole("alert").filter({ hasText: /denied|permissions/i }),
  ).toBeVisible();
  await expect(notes(page)).toHaveValue("Retry draft");
  await page.unroute(/\/documents:commit(?:\?|$)/);
  await saveButton(page).click();
  await expect(status(page)).toContainText("Availability saved");
  await expect(saveButton(page)).toBeDisabled();
  await admin(async (ctx) => {
    const saved = (await getDoc(doc(ctx.firestore(), path))).data()!;
    expect(saved.version).toBe(4);
    expect(saved.notes).toBe("Retry draft");
  });
  // A concurrent writer can independently save exactly the draft's desired value.
  await page
    .getByRole("combobox", { name: "Before break", exact: true })
    .selectOption("automatic");
  await notes(page).fill("Same value");
  await override("Same value", 5);
  await expect(
    page.getByRole("button", { name: /Example Alpha/ }),
  ).not.toContainText("Override");
  await saveButton(page).click();
  await expect(status(page)).toContainText("Availability saved");
  await expect(saveButton(page)).toBeDisabled();
  await notes(page).fill("After matching remote save");
  await saveButton(page).click();
  await expect(status(page)).toContainText("Availability saved");
  await expect(saveButton(page)).toBeDisabled();
  await admin(async (ctx) => {
    expect((await getDoc(doc(ctx.firestore(), path))).data()?.version).toBe(6);
  });
});
test("protects player/night/tab/page drafts and supports keyboard navigation on a narrow screen", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await open(page);
  const before = page.getByRole("combobox", {
    name: "Before break",
    exact: true,
  });
  await before.selectOption("available");
  await before.focus();
  await page.keyboard.press("Tab");
  await expect(
    page.getByRole("combobox", { name: "After break", exact: true }),
  ).toBeFocused();
  await notes(page).focus();
  await page.keyboard.press("ControlOrMeta+A");
  await page.keyboard.type("Keyboard draft");
  await page.keyboard.press("Tab");
  await expect(saveButton(page)).toBeFocused();
  await expect(before).toHaveValue("available");
  for (const leave of [
    () => page.getByRole("button", { name: /Example Beta/ }).click(),
    () =>
      page
        .getByRole("combobox", { name: "Raid night", exact: true })
        .selectOption(second),
    () =>
      page
        .getByRole("navigation", { name: "Main navigation" })
        .getByRole("button", { name: "Review Night", exact: true })
        .click(),
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
    await expect(before).toHaveValue("available");
  }
  await page.getByRole("button", { name: /Example Beta/ }).click();
  await page.getByRole("button", { name: "Discard edits" }).click();
  await expect(
    page.getByRole("heading", { name: "Example Beta", exact: true }),
  ).toBeVisible();
  await expect(notes(page)).toHaveValue("Other player");
  await page.getByRole("button", { name: /Example Alpha/ }).click();
  await page
    .getByRole("combobox", { name: "Raid night", exact: true })
    .selectOption(second);
  await expect(notes(page)).toHaveValue("Other night");
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});

test("handles roster arrival, clean removal, search and unknown metadata", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await admin(async (ctx) => {
    await deleteDoc(doc(ctx.firestore(), `${season}/players/player-a`));
    await deleteDoc(doc(ctx.firestore(), `${season}/players/player-b`));
  });
  await officer(page, emulator.environment);
  await page
    .getByRole("navigation")
    .getByRole("button", { name: "Overrides", exact: true })
    .click();
  await expect(page.getByText("No players in the roster yet.")).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Save adjustment" }),
  ).toHaveCount(0);
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), `${season}/players/player-a`), {
      name: "Example Alpha",
      className: "Mage",
      role: "Unknown",
      active: true,
      ...metadata(1),
    });
  });
  const editor = page.getByRole("region", {
    name: "Player availability editor",
  });
  await expect(
    editor.getByRole("heading", { name: "Example Alpha", exact: true }),
  ).toBeVisible();
  await expect(editor.locator(".editor-heading p")).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: /Example Alpha/ }),
  ).not.toContainText("Unknown");
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), `${season}/players/player-b`), {
      name: "Example Beta",
      className: "",
      role: "Healer",
      active: true,
      ...metadata(1),
    });
  });
  const search = page.getByRole("textbox", { name: "Search players" });
  await search.fill("Beta");
  await expect(
    editor.getByRole("heading", { name: "Example Alpha", exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: /Example Beta/ }).focus();
  await page.keyboard.press("Enter");
  await expect(editor.locator(".editor-heading p")).toHaveCount(0);
  await search.fill("no match");
  await expect(
    page.getByText("No matching players. Try another search."),
  ).toBeVisible();
  await expect(
    editor.getByRole("heading", { name: "Example Beta", exact: true }),
  ).toBeVisible();
  await admin(async (ctx) => {
    await deleteDoc(doc(ctx.firestore(), `${season}/players/player-b`));
    await setDoc(
      doc(ctx.firestore(), `${season}/players/player-a`),
      { className: " Unknown ", role: "" },
      { merge: true },
    );
  });
  await expect(
    editor.getByRole("heading", { name: "Example Alpha", exact: true }),
  ).toBeVisible();
  await expect(editor.locator(".editor-heading p")).toHaveCount(0);
  await search.fill("");
  await expect(
    page.getByRole("button", { name: /Example Alpha/ }),
  ).not.toContainText(/Unknown| · /);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});

test("preserves a removed player's draft and recovers after cancel without writing it", async ({
  page,
}) => {
  await open(page);
  await notes(page).fill("Retained roster draft");
  const removeAlpha = () =>
    admin(async (ctx) => {
      await deleteDoc(doc(ctx.firestore(), `${season}/players/player-a`));
    });
  await removeAlpha();
  await expect(
    page.getByText(/This player is no longer in the roster/),
  ).toBeVisible();
  await expect(saveButton(page)).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Run Pebble", exact: true }),
  ).toBeDisabled();
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), `${season}/players/player-a`), {
      name: "Example Alpha",
      className: "Mage",
      role: "Damage",
      active: true,
      ...metadata(1),
    });
  });
  await expect(notes(page)).toHaveValue("Retained roster draft");
  await removeAlpha();
  await page.getByRole("button", { name: "Cancel edits", exact: true }).focus();
  await page.keyboard.press("Enter");
  await expect(
    page.getByRole("heading", { name: "Example Beta", exact: true }),
  ).toBeVisible();
  await expect(notes(page)).toHaveValue("Other player");
  await admin(async (ctx) => {
    expect((await getDoc(doc(ctx.firestore(), path))).data()?.notes).toBe(
      "Saved Alpha",
    );
    expect((await getDoc(doc(ctx.firestore(), path))).data()?.version).toBe(1);
    await deleteDoc(doc(ctx.firestore(), `${season}/players/player-b`));
  });
  await expect(page.getByText("No players in the roster yet.")).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "Select a player", exact: true }),
  ).toBeVisible();
});
