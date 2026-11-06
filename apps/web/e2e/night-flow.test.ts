import { expect, test } from "@playwright/test";
import {
  collection,
  doc,
  getDocs,
  query,
  setDoc,
  Timestamp,
  where,
  writeBatch,
} from "firebase/firestore";
import { setupEmulator } from "./support/emulator";
import { metadata, seedHeartbeat } from "./support/fixtures";
import { officer } from "./support/officer";

const emulator = setupEmulator();
const season = "seasons/test-reports";
const selectedNight = "2026-10-14";
const reportNights = {
  AAAAAAAAAAAAAAAA: selectedNight,
  BBBBBBBBBBBBBBBB: "2026-10-15",
};
const player = {
  id: "player-a",
  name: "Example Player",
  className: "Mage",
  role: "Damage",
};
const inputVersions = {
  ...Object.fromEntries(
    Object.keys(reportNights).map((code) => [`reports/${code}`, 1]),
  ),
  [`players/${player.id}`]: 1,
};

// Stand in for the worker only after the browser has queued a real request.
async function completeRun(versions: Record<string, number>) {
  let runId = "";
  await expect
    .poll(async () => {
      let count = 0;
      await emulator.admin(async (ctx) => {
        const queued = await getDocs(
          query(
            collection(ctx.firestore(), `${season}/runs`),
            where("status", "==", "queued"),
          ),
        );
        runId = queued.docs[0]?.id ?? "";
        count = queued.size;
      });
      return count;
    })
    .toBe(1);

  await emulator.admin(async (ctx) => {
    const db = ctx.firestore();
    const batch = writeBatch(db);
    const completedAt = Timestamp.now();
    const result = `${season}/results/${runId}`;
    const row = {
      ...player,
      color: "#b9c7d8",
      played: 90,
      bench: 30,
      preBench: 10,
      postBench: 20,
    };
    batch.set(doc(db, result), { complete: true, completedAt, reportCount: 2 });
    batch.set(doc(db, `${result}/players/${player.id}`), row);
    for (const night of Object.values(reportNights)) {
      batch.set(doc(db, `${result}/nights/${night}`), {
        night,
        duration: 170,
        reports: 1,
        players: [row],
        timing: {
          start: `${night}T19:00`,
          end: `${night}T22:00`,
          breakStart: `${night}T20:30`,
          breakEnd: `${night}T20:40`,
          notes: "",
        },
      });
    }
    batch.update(doc(db, `${season}/runs/${runId}`), {
      status: "succeeded",
      completedAt,
      inputVersions: versions,
      reportSummaries: Object.fromEntries(
        Object.entries(reportNights).map(([code, night]) => [
          code,
          { night, title: "Test raid" },
        ]),
      ),
    });
    batch.set(doc(db, "publicSeasons/test-reports"), { latestRunId: runId });
    await batch.commit();
  });
}

test("guides an officer through four steps, recalculates edits, and finishes on the selected night", async ({
  page,
}) => {
  await emulator.admin(async (ctx) => {
    const db = ctx.firestore();
    await seedHeartbeat(db);
    await setDoc(doc(db, `${season}/players/${player.id}`), {
      name: player.name,
      className: player.className,
      role: player.role,
      active: true,
      joinDate: "",
      leaveDate: "",
      ...metadata(),
    });
  });
  await officer(page, emulator.environment);
  const flow = page.getByRole("navigation", { name: "Process a raid night" });
  await expect(flow.getByRole("listitem")).toHaveCount(4);
  await expect(page.getByText(/Selected raid night:/)).toHaveCount(0);
  const reportInput = page.getByRole("textbox", {
    name: "Warcraft Logs reports",
  });
  for (const code of Object.keys(reportNights)) {
    await reportInput.fill(code);
    await page.getByRole("button", { name: "Add report", exact: true }).click();
    await expect(reportInput).toHaveValue("");
  }
  await page
    .getByRole("button", { name: "Next: Run Pebble and go to Review Night" })
    .click();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Reports");
  await expect(flow.locator('[aria-current="step"]')).toContainText(
    "Add report",
  );
  await completeRun(inputVersions);
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(
    "Review Night",
    { timeout: 20000 },
  );
  const night = page.getByRole("combobox", { name: "Raid night", exact: true });
  await night.selectOption(selectedNight);
  await page
    .getByText("Times look wrong? Adjust timing", { exact: true })
    .click();
  await page
    .getByRole("textbox", { name: "Timing notes" })
    .fill("Reviewed break and raid times");
  await page.getByRole("button", { name: "Save timing", exact: true }).click();
  await page
    .getByRole("button", { name: "Recalculate and review overrides →" })
    .click();
  const timingVersions = {
    ...inputVersions,
    [`nightOverrides/${selectedNight}`]: 1,
  };
  await completeRun(timingVersions);
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(
    "Overrides",
    { timeout: 20000 },
  );
  await expect(night).toHaveValue(selectedNight);
  await page
    .getByRole("textbox", { name: "Availability notes" })
    .fill("Reviewed availability");
  await page
    .getByRole("button", { name: "Save availability", exact: true })
    .click();
  await page
    .getByRole("button", { name: "Recalculate and view Bench Time →" })
    .click();
  await completeRun({
    ...timingVersions,
    [`players/${player.id}`]: 2,
    [`availabilityOverrides/${selectedNight}_${player.id}`]: 1,
  });
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(
    "Bench Time",
    {
      timeout: 20000,
    },
  );
  await expect(
    page.getByRole("combobox", { name: "Result period" }),
  ).toHaveValue(selectedNight);
  await expect(
    page.getByRole("status", { name: "Night completion" }),
  ).toHaveCount(0);
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("button", { name: /^Reports/ })
    .click();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Reports");
  await expect(
    page.getByRole("navigation", { name: "Main navigation" }),
  ).toBeVisible();
});
