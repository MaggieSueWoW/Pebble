import { assertFails } from "@firebase/rules-unit-testing";
import { expect, test } from "@playwright/test";
import { doc, getDoc, Timestamp, writeBatch } from "firebase/firestore";
import { setupEmulator } from "./support/emulator";
import { officer } from "./support/officer";

const emulator = setupEmulator();
const season = "seasons/test-reports",
  night = "2026-10-15";
test("public recorded explanations and officer-only diagnostics survive selection and clear on sign-out", async ({
  page,
}) => {
  await emulator.admin(async (ctx) => {
    const db = ctx.firestore(),
      batch = writeBatch(db),
      at = Timestamp.now();
    const row = {
      id: "example",
      name: "Example Player",
      className: "Mage",
      role: "Damage",
      color: "#b9c7d8",
      played: 60,
      bench: 30,
      preBench: 10,
      postBench: 20,
      overridden: true,
      nights: 1,
    };
    batch.set(doc(db, `${season}/results/ready`), {
      complete: true,
      completedAt: at,
      reportCount: 1,
    });
    batch.set(doc(db, `${season}/results/ready/players/example`), row);
    batch.set(doc(db, `${season}/results/ready/nights/${night}`), {
      night,
      duration: 180,
      reports: 1,
      timing: {
        start: `${night}T19:00`,
        breakStart: `${night}T20:00`,
        breakEnd: `${night}T20:15`,
        end: `${night}T22:00`,
        notes: "",
      },
      players: [
        {
          ...row,
          availability: {
            pre: { available: true, mode: "partial" },
            post: { available: false, mode: "automatic" },
            source: "override",
          },
        },
      ],
    });
    batch.set(doc(db, `${season}/runs/ready`), {
      status: "succeeded",
      requestedBy: "fixture",
      requestedAt: at,
      completedAt: at,
      inputVersions: {},
      nightDiagnostics: {
        [night]: {
          reportCount: 1,
          mythicFights: 4,
          unmappedCharacters: 2,
          breakMinutes: 15,
          timing: { start: "configured", end: "detected", break: "override" },
        },
      },
    });
    batch.set(doc(db, "publicSeasons/test-reports"), { latestRunId: "ready" });
    await batch.commit();
  });
  await assertFails(
    getDoc(
      doc(
        emulator.environment.unauthenticatedContext().firestore(),
        `${season}/runs/ready`,
      ),
    ),
  );
  await assertFails(
    getDoc(
      doc(
        emulator.environment.authenticatedContext("non-officer").firestore(),
        `${season}/runs/ready`,
      ),
    ),
  );
  await page.goto(`/?view=results&period=${night}&player=example`);
  const explanation = page.getByRole("region", {
    name: "Recorded availability explanation",
  });
  await page.getByText("Availability explanation", { exact: true }).click();
  await expect(explanation).toContainText("Partial availability");
  await expect(explanation).toContainText("Recorded unavailable");
  await expect(
    page.getByLabel("Officer night diagnostics", { exact: true }),
  ).toHaveCount(0);
  await officer(page, emulator.environment);
  await page
    .getByRole("navigation")
    .getByRole("button", { name: "Results", exact: true })
    .click();
  await page
    .getByRole("combobox", { name: "Result period" })
    .selectOption(night);
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("button", { name: "Review Night", exact: true })
    .click();
  const diagnostics = page.getByLabel("Officer night diagnostics", {
    exact: true,
  });
  await expect(diagnostics).toBeVisible();
  await diagnostics.locator("summary").focus();
  await page.keyboard.press("Enter");
  await expect(diagnostics).toContainText("Configured default");
  await expect(diagnostics).toContainText("Officer timing override");
  await expect(diagnostics.locator("dd").nth(2)).toHaveText("2");
  await page.setViewportSize({ width: 390, height: 844 });
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("button", { name: "Results", exact: true })
    .click();
  await expect(diagnostics).toHaveCount(0);
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("button", { name: "Review Night", exact: true })
    .click();
  await expect(diagnostics).toBeVisible();
  await page.setViewportSize({ width: 1280, height: 900 });
  await page.getByRole("button", { name: "Sign out", exact: true }).click();
  await expect(diagnostics).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Details for Example Player" }),
  ).toBeVisible();
});
