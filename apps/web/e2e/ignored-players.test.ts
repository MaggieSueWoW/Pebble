import { expect, test } from "@playwright/test";
import { doc, getDoc, setDoc } from "firebase/firestore";
import {
  setupEmulator,
  setEmulatorRules,
  firestoreRules,
} from "./support/emulator";
import { metadata, seedHeartbeat } from "./support/fixtures";
import { officer } from "./support/officer";

const emulator = setupEmulator();
const season = "seasons/test-reports";

test("lists season ignores after reload and undoes by keyboard without changing other inputs", async ({
  page,
}) => {
  await emulator.admin(async (ctx) => {
    const db = ctx.firestore();
    await seedHeartbeat(db);
    for (const [id, name, region] of [
      ["guest", "Guest", "eu"],
      ["visitor", "Visitor", "us"],
    ])
      await setDoc(doc(db, `${season}/characters/${id}`), {
        name,
        realm: "Example",
        region,
        playerId: "",
        ignored: true,
        ...metadata(),
      });
    await setDoc(doc(db, "seasons/other/characters/guest"), {
      name: "Otherseason",
      realm: "Example",
      region: "eu",
      playerId: "",
      ignored: true,
      ...metadata(),
    });
  });
  await officer(page, emulator.environment);
  await page
    .getByRole("navigation")
    .getByRole("button", { name: "Roster", exact: true })
    .click();
  const panel = page.getByRole("region", { name: "Ignored players (2)" });
  await expect(panel).toBeVisible();
  await expect(panel).not.toContainText("Otherseason");
  await page.reload();
  await expect(panel).toBeVisible();
  await page.setViewportSize({ width: 390, height: 844 });
  await page
    .getByRole("textbox", { name: "Search ignored players" })
    .fill("EU");
  await expect(panel.getByText("Visitor", { exact: true })).toHaveCount(0);
  const undo = panel.getByRole("button", {
    name: "Undo ignore for Guest (Example, EU)",
  });
  await undo.focus();
  await page.keyboard.press("Enter");
  await expect(
    page.getByRole("region", { name: "Ignored players (1)" }),
  ).toContainText("Run Pebble");
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  await page.reload();
  await expect(
    page.getByRole("region", { name: "Ignored players (1)" }),
  ).toContainText("Visitor");
  await emulator.admin(async (ctx) => {
    const db = ctx.firestore();
    expect((await getDoc(doc(db, `${season}/characters/guest`))).exists()).toBe(
      false,
    );
    expect(
      (await getDoc(doc(db, `${season}/characters/visitor`))).data()?.version,
    ).toBe(1);
    expect(
      (await getDoc(doc(db, "seasons/other/characters/guest"))).exists(),
    ).toBe(true);
  });
});

test("keeps an ignore visible and retryable when the server rejects undo", async ({
  page,
}) => {
  await emulator.admin(async (ctx) => {
    const db = ctx.firestore();
    await seedHeartbeat(db);
    await setDoc(doc(db, `${season}/characters/guest`), {
      name: "Guest",
      realm: "Example",
      region: "eu",
      playerId: "",
      ignored: true,
      ...metadata(),
    });
  });
  await officer(page, emulator.environment);
  await page
    .getByRole("navigation")
    .getByRole("button", { name: "Roster", exact: true })
    .click();
  const panel = page.getByRole("region", { name: "Ignored players (1)" });
  const undo = panel.getByRole("button", { name: /Undo ignore for Guest/ });
  await expect(undo).toBeVisible();
  await setEmulatorRules(
    firestoreRules.replaceAll(
      "allow delete: if officer() && validId(season) && validId(id)",
      "allow delete: if false && officer() && validId(season) && validId(id)",
    ),
  );
  try {
    await undo.click();
    await expect(panel.getByRole("alert")).toBeVisible();
    await expect(undo).toBeEnabled();
    await emulator.admin(async (ctx) => {
      expect(
        (
          await getDoc(doc(ctx.firestore(), `${season}/characters/guest`))
        ).exists(),
      ).toBe(true);
    });
  } finally {
    await setEmulatorRules(firestoreRules);
  }
  await undo.click();
  await expect(
    page.getByRole("region", { name: "Ignored players (0)" }),
  ).toContainText("No ignored players this season.");
});
