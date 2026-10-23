import { expect, test } from "@playwright/test";
import {
  collection,
  doc,
  getDocs,
  setDoc,
  Timestamp,
} from "firebase/firestore";
import { setupEmulator } from "./support/emulator";
import { metadata, seedHeartbeat } from "./support/fixtures";
import { officer } from "./support/officer";

const emulator = setupEmulator();
const season = "seasons/test-reports";
const night = "2026-10-21";
test("reviews times and resolves each extra player without leaving the night", async ({
  page,
}) => {
  await emulator.admin(async (context) => {
    const db = context.firestore();
    const at = Timestamp.now();
    await seedHeartbeat(db);
    await setDoc(doc(db, `${season}/players/main`), {
      name: "Existing",
      className: "Mage",
      role: "Damage",
      active: true,
      joinDate: "",
      leaveDate: "",
      ...metadata(),
    });
    await setDoc(doc(db, `${season}/reports/AAAAAAAAAAAAAAAA`), {
      enabled: true,
      notes: "",
      ...metadata(),
    });
    const timing = {
      start: `${night}T19:00`,
      end: `${night}T22:00`,
      breakStart: `${night}T20:30`,
      breakEnd: `${night}T20:40`,
      notes: "",
    };
    await setDoc(doc(db, `${season}/results/ready`), {
      complete: true,
      completedAt: at,
      reportCount: 1,
      warnings: [
        `${night}: unknown characters: eu:Azjol Nerub:Newmain, eu:Azjol Nerub:Newalt, eu:Azjol Nerub:Guest`,
      ],
    });
    await setDoc(doc(db, `${season}/results/ready/nights/${night}`), {
      night,
      duration: 170,
      reports: 1,
      timing,
      players: [],
    });
    await setDoc(doc(db, `${season}/runs/ready`), {
      status: "succeeded",
      requestedBy: "fixture",
      requestedAt: at,
      completedAt: at,
      inputVersions: { "players/main": 1, "reports/AAAAAAAAAAAAAAAA": 1 },
      reportSummaries: { AAAAAAAAAAAAAAAA: { night, title: "Test raid" } },
      nightDiagnostics: {
        [night]: {
          reportCount: 1,
          mythicFights: 3,
          unmappedCharacters: 3,
          breakMinutes: 10,
          timing: { start: "detected", end: "detected", break: "detected" },
          missingPlayers: ["Newmain", "Newalt", "Guest"].map((name) => ({
            name,
            realm: "Azjol-Nerub",
            region: "eu",
            className: "Mage",
          })),
        },
      },
    });
    await setDoc(doc(db, "publicSeasons/test-reports"), {
      latestRunId: "ready",
    });
  });
  await officer(page, emulator.environment);
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("button", { name: "Review Night", exact: true })
    .click();
  await expect(page.getByText(/^7:00 PMDetected$/)).toBeVisible();
  await expect(
    page.getByText(/^8:30 PM – 8:40 PM \(10 min\)Detected$/),
  ).toBeVisible();
  await expect(
    page.getByRole("region", { name: "Result warnings" }),
  ).toHaveCount(0);
  const main = page.getByRole("article", { name: "Missing player Newmain" });
  const summary = page.getByRole("region", { name: "Raid night summary" });
  await expect(summary).toContainText("Mythic encounters");
  await expect(summary).toContainText("Detected");
  const next = page.getByRole("button", {
    name: "Next: Review overrides →",
    exact: true,
  });
  await expect(next).toHaveCount(1);
  const summaryBox = (await summary.boundingBox())!;
  const nextBox = (await next.boundingBox())!;
  const missingBox = (await page
    .getByRole("region", { name: "Missing players", exact: true })
    .boundingBox())!;
  expect(nextBox.y).toBeGreaterThanOrEqual(summaryBox.y + summaryBox.height);
  expect(nextBox.y + nextBox.height).toBeLessThanOrEqual(missingBox.y);
  const rowHeight = (await main.boundingBox())!.height;
  await main.getByRole("button", { name: "Add main", exact: true }).click();
  expect((await main.boundingBox())!.height).toBe(rowHeight);
  await expect(main.getByText(`Join date: ${night}`)).toBeVisible();
  await expect(
    main.getByRole("combobox", { name: "Role for Newmain" }),
  ).toHaveCount(0);
  await main.getByRole("button", { name: "Save main", exact: true }).click();
  await expect(main).toContainText(`joins ${night}`);
  const alt = page.getByRole("article", { name: "Missing player Newalt" });
  await alt.getByRole("button", { name: "Add alt", exact: true }).click();
  expect((await alt.boundingBox())!.height).toBe(rowHeight);
  await alt
    .getByRole("combobox", { name: "Main for Newalt" })
    .selectOption("main");
  await alt.getByRole("button", { name: "Save alt", exact: true }).click();
  await expect(alt).toContainText("Mapped to Existing");
  const guest = page.getByRole("article", { name: "Missing player Guest" });
  await guest
    .getByRole("button", { name: "Ignore forever", exact: true })
    .click();
  await expect(
    guest.getByRole("button", { name: "Confirm ignore" }),
  ).toBeVisible();
  expect((await guest.boundingBox())!.height).toBe(rowHeight);
  await emulator.admin(async (context) => {
    const entries = await getDocs(
      collection(context.firestore(), `${season}/characters`),
    );
    expect(entries.docs.some((entry) => entry.data().name === "Guest")).toBe(
      false,
    );
  });
  await guest.getByRole("button", { name: "Cancel player change" }).click();
  await expect(
    guest.getByRole("button", { name: "Ignore forever" }),
  ).toBeVisible();
  await guest.getByRole("button", { name: "Ignore forever" }).click();
  await guest.getByRole("button", { name: "Confirm ignore" }).click();
  await expect(guest).toContainText("Ignored for this season");
  await page.reload();
  await expect(
    page.getByRole("article", { name: "Missing player Guest" }),
  ).toContainText("Ignored for this season");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(
    "Review Night",
  );
  await emulator.admin(async (context) => {
    const db = context.firestore();
    const players = (
      await getDocs(collection(db, `${season}/players`))
    ).docs.map((entry) => entry.data());
    expect(players.find((entry) => entry.name === "Newmain")).toMatchObject({
      joinDate: night,
      active: true,
      className: "Mage",
      role: "Unknown",
    });
    const characters = (
      await getDocs(collection(db, `${season}/characters`))
    ).docs.map((entry) => entry.data());
    expect(characters.find((entry) => entry.name === "Newalt")).toMatchObject({
      realm: "Azjol-Nerub",
      region: "eu",
      playerId: "main",
    });
    expect(characters.find((entry) => entry.name === "Guest")).toMatchObject({
      realm: "Azjol-Nerub",
      region: "eu",
      ignored: true,
      playerId: "",
    });
    expect((await getDocs(collection(db, `${season}/runs`))).size).toBe(1);
  });
  await page.setViewportSize({ width: 390, height: 844 });
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});
