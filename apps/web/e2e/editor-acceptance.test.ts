import { test, expect } from "@playwright/test";
import { initializeTestEnvironment } from "@firebase/rules-unit-testing";
import {
  collection,
  doc,
  getDocs,
  setDoc,
  Timestamp,
  writeBatch,
} from "firebase/firestore";
import { readFileSync } from "node:fs";
import { officer } from "./officer";

test("saved roster, mapping and settings feed one explicit run and remain stale until published results load", async ({
  page,
}) => {
  const env = await initializeTestEnvironment({
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
  const season = "seasons/test-reports",
    report = "AAAAAAAAAAAAAAAA";
  const admin = (fn: Parameters<typeof env.withSecurityRulesDisabled>[0]) =>
    env.withSecurityRulesDisabled(fn);
  const run = page.getByRole("button", { name: "Run Pebble", exact: true });
  const status = page.getByRole("status", { name: "Run status" });
  const meta = { version: 1, updatedBy: "fixture", updatedAt: Timestamp.now() };
  const resultRow = {
    id: "previous",
    name: "Previous",
    className: "Mage",
    role: "Damage",
    color: "#b9c7d8",
    played: 20,
    bench: 10,
    preBench: 5,
    postBench: 5,
    nights: 1,
    overridden: false,
  };
  let release = () => {};
  try {
    await env.clearFirestore();
    await admin(async (ctx) => {
      const db = ctx.firestore(),
        completedAt = Timestamp.fromMillis(Date.now() - 60000);
      await setDoc(doc(db, `${season}/reports/${report}`), {
        enabled: true,
        ...meta,
      });
      await setDoc(doc(db, `${season}/results/baseline`), {
        complete: true,
        completedAt,
        reportCount: 1,
      });
      await setDoc(
        doc(db, `${season}/results/baseline/players/previous`),
        resultRow,
      );
      await setDoc(doc(db, `${season}/runs/baseline`), {
        status: "succeeded",
        requestedBy: "fixture",
        requestedAt: completedAt,
        completedAt,
        inputVersions: { [`reports/${report}`]: 1 },
      });
      await setDoc(doc(db, "publicSeasons/test-reports"), {
        latestRunId: "baseline",
      });
      await setDoc(doc(db, "bot/status"), { lastHeartbeatAt: Timestamp.now() });
    });
    await officer(page, env);
    await page
      .getByRole("navigation")
      .getByRole("button", { name: "Roster", exact: true })
      .click();
    await page.getByRole("button", { name: "Add main", exact: true }).click();
    await page.getByLabel("Main name", { exact: true }).fill("Example");
    await expect(run).toBeDisabled();
    await page.getByRole("button", { name: "Save main", exact: true }).click();
    await expect(
      page.getByRole("status", { name: "Roster changes" }),
    ).toContainText("Roster saved");
    await page.getByLabel("Main name", { exact: true }).fill("Renamed");
    await page.getByLabel("Class", { exact: true }).selectOption("Druid");
    await expect(run).toBeDisabled();
    await page.getByRole("button", { name: "Save main", exact: true }).click();
    await expect(
      page.getByRole("status", { name: "Roster changes" }),
    ).toContainText("Roster saved");
    let mainId = "";
    await admin(async (ctx) => {
      mainId = (await getDocs(collection(ctx.firestore(), `${season}/players`)))
        .docs[0].id;
    });
    await page.getByRole("button", { name: "Add alt", exact: true }).click();
    await page.getByLabel("Character name", { exact: true }).fill("Loggedalt");
    await page.getByLabel("Realm", { exact: true }).fill("Example Realm");
    await expect(run).toBeDisabled();
    await page.getByRole("button", { name: "Save alt", exact: true }).click();
    await expect(
      page.getByRole("status", { name: "Character mapping changes" }),
    ).toContainText("Character mapping saved");
    let mappingId = "";
    await admin(async (ctx) => {
      mappingId = (
        await getDocs(collection(ctx.firestore(), `${season}/characters`))
      ).docs[0].id;
    });
    await page
      .getByRole("navigation")
      .getByRole("button", { name: "Settings", exact: true })
      .click();
    await page.getByLabel("Post extension minutes", { exact: true }).fill("0");
    await expect(run).toBeDisabled();
    await page
      .getByRole("button", { name: "Save calculation settings", exact: true })
      .click();
    await expect(
      page.getByRole("status", { name: "Calculation settings changes" }),
    ).toContainText("Calculation settings saved");
    const versions = {
      [`reports/${report}`]: 1,
      [`players/${mainId}`]: 2,
      [`characters/${mappingId}`]: 1,
      "settings/calculation": 1,
    };
    await admin(async (ctx) => {
      const db = ctx.firestore();
      expect(
        (await getDocs(collection(db, `${season}/runs`))).docs.map((d) => d.id),
      ).toEqual(["baseline"]);
      expect(
        (await getDocs(collection(db, `${season}/players`))).docs[0].data(),
      ).toMatchObject({
        name: "Renamed",
        className: "Druid",
        role: "Unknown",
        version: 2,
      });
      expect(
        (await getDocs(collection(db, `${season}/characters`))).docs[0].data(),
      ).toMatchObject({
        name: "Loggedalt",
        realm: "Example Realm",
        region: "us",
        playerId: mainId,
        version: 1,
      });
      expect(
        (await getDocs(collection(db, `${season}/settings`))).docs[0].data(),
      ).toMatchObject({ postExtensionMinutes: 0, version: 1 });
    });
    await page
      .getByRole("navigation")
      .getByRole("button", { name: /^Results/ })
      .click();
    await expect(page.getByText("Previous", { exact: true })).toBeVisible();
    await expect(status).toContainText("Changes ready for the next run");
    await page
      .getByRole("navigation")
      .getByRole("button", { name: /^Reports/ })
      .click();
    await run.click();
    await expect(
      page.getByRole("button", { name: "Run queued", exact: true }),
    ).toBeDisabled();
    let requestId = "";
    await admin(async (ctx) => {
      const requests = (
        await getDocs(collection(ctx.firestore(), `${season}/runs`))
      ).docs.filter((d) => d.id !== "baseline");
      expect(requests).toHaveLength(1);
      expect(requests[0].data()).toMatchObject({ status: "queued" });
      expect(requests[0].data().requestedBy).toBeTruthy();
      requestId = requests[0].id;
      await setDoc(
        requests[0].ref,
        {
          status: "running",
          stage: "Calculating results",
          inputVersions: versions,
        },
        { merge: true },
      );
    });
    await expect(status).toContainText("Calculating results");
    await page
      .getByRole("navigation")
      .getByRole("button", { name: "Results", exact: true })
      .click();
    await expect(page.getByText("Previous", { exact: true })).toBeVisible();
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let heldReads = 0;
    await page.route(
      /google\.firestore\.v1\.Firestore\/Listen\/channel/,
      async (route) => {
        const request = route.request();
        if (
          request.method() === "POST" &&
          [...new URLSearchParams(request.postData() ?? "").values()].some(
            (value) => value.includes(`${season}/results/${requestId}`),
          )
        ) {
          heldReads++;
          await gate;
        }
        await route.continue();
      },
    );
    await admin(async (ctx) => {
      const db = ctx.firestore(),
        batch = writeBatch(db),
        completedAt = Timestamp.now();
      batch.set(doc(db, `${season}/results/${requestId}`), {
        complete: true,
        completedAt,
        reportCount: 1,
      });
      batch.set(doc(db, `${season}/results/${requestId}/players/${mainId}`), {
        ...resultRow,
        id: mainId,
        name: "Renamed",
        className: "Druid",
        role: "Unknown",
      });
      batch.set(
        doc(db, `${season}/runs/${requestId}`),
        { status: "succeeded", completedAt },
        { merge: true },
      );
      batch.set(doc(db, "publicSeasons/test-reports"), {
        latestRunId: requestId,
      });
      await batch.commit();
    });
    await expect.poll(() => heldReads).toBeGreaterThan(0);
    await expect(status).toContainText("Loading latest results");
    await expect(page.getByText("Previous", { exact: true })).toBeVisible();
    await expect(page.getByText("Renamed", { exact: true })).toHaveCount(0);
    release();
    await expect(page.getByText("Renamed", { exact: true })).toBeVisible();
    await expect(page.getByText("Previous", { exact: true })).toHaveCount(0);
    await expect(status).toContainText("Updated");
    await admin(async (ctx) => {
      expect(
        (await getDocs(collection(ctx.firestore(), `${season}/runs`))).size,
      ).toBe(2);
    });
  } finally {
    release();
    await env.cleanup();
  }
});
