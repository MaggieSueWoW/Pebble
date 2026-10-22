import { test, expect, type Page } from "@playwright/test";
import {
  initializeTestEnvironment,
  type RulesTestEnvironment,
} from "@firebase/rules-unit-testing";
import {
  collection,
  deleteDoc,
  doc,
  getDoc,
  getDocs,
  setDoc,
  Timestamp,
  writeBatch,
} from "firebase/firestore";
import { readFileSync } from "node:fs";
import { officer, signIn } from "./officer";
const season = "seasons/test-reports",
  report = "AAAAAAAAAAAAAAAA";
const captured = { [`reports/${report}`]: 1, "players/player-a": 1 };
const nights = {
  baseline: "2026-10-14",
  delayed: "2026-10-15",
  latest: "2026-10-16",
};
let env: RulesTestEnvironment,
  expectedRequests = 0,
  expectedReportVersion = 1;
let releases: (() => void)[] = [];
const admin = (
  fn: Parameters<RulesTestEnvironment["withSecurityRulesDisabled"]>[0],
) => env.withSecurityRulesDisabled(fn);
const status = (page: Page) => page.getByRole("status", { name: "Run status" });
async function seedResult(
  id: string,
  name: string,
  night: string,
  complete = true,
  reports = 2,
  warnings?: string[],
) {
  await admin(async (ctx) => {
    const db = ctx.firestore(),
      batch = writeBatch(db);
    const row = {
      id: "player-a",
      name,
      className: "Mage",
      role: "Damage",
      color: "#b9c7d8",
      played: reports * 60,
      bench: 30,
      preBench: 10,
      postBench: 20,
      overridden: false,
      nights: 1,
    };
    batch.set(doc(db, `${season}/results/${id}`), {
      complete,
      reportCount: reports,
      ...(warnings ? { warnings } : {}),
      completedAt: Timestamp.now(),
    });
    batch.set(doc(db, `${season}/results/${id}/players/player-a`), row);
    batch.set(doc(db, `${season}/results/${id}/nights/${night}`), {
      night,
      duration: 180,
      reports,
      players: [row],
      timing: {
        start: `${night}T19:00`,
        breakStart: `${night}T20:00`,
        breakEnd: `${night}T20:15`,
        end: `${night}T22:00`,
        notes: "",
      },
    });
    await batch.commit();
  });
}
async function publish(id: string, inputVersions = captured) {
  await admin(async (ctx) => {
    const db = ctx.firestore(),
      batch = writeBatch(db),
      ref = doc(db, `${season}/runs/${id}`);
    const exists = (await getDoc(ref)).exists();
    batch.set(
      ref,
      {
        ...(!exists
          ? { requestedBy: "fixture", requestedAt: Timestamp.now() }
          : {}),
        status: "succeeded",
        completedAt: Timestamp.now(),
        inputVersions,
      },
      { merge: true },
    );
    batch.set(doc(db, "publicSeasons/test-reports"), { latestRunId: id });
    await batch.commit();
  });
}
async function resultsPage(page: Page, signedIn = true) {
  if (signedIn) {
    await officer(page, env);
    await page
      .getByRole("navigation")
      .getByRole("button", { name: /^Results/ })
      .click();
  } else await page.goto("/");
  await expect(
    page.getByText("Baseline Player", { exact: true }),
  ).toBeVisible();
}
async function holdReads(page: Page, id: string) {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  releases.push(release);
  const state = { hits: 0, release };
  await page.route(
    /google\.firestore\.v1\.Firestore\/Listen\/channel/,
    async (route) => {
      const request = route.request();
      const values = [
        ...new URLSearchParams(request.postData() ?? "").values(),
      ];
      if (
        request.method() === "POST" &&
        values.some((value) => value.includes(`${season}/results/${id}`))
      ) {
        state.hits++;
        await gate;
      }
      await route.continue();
    },
  );
  return state;
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
  releases = [];
  expectedRequests = 0;
  expectedReportVersion = 1;
  await env.clearFirestore();
  await seedResult("baseline", "Baseline Player", nights.baseline, true, 1);
  await admin(async (ctx) => {
    const db = ctx.firestore(),
      at = Timestamp.fromMillis(Date.now() - 3600000);
    await setDoc(doc(db, `${season}/reports/${report}`), {
      enabled: true,
      notes: "Baseline",
      version: 1,
      updatedBy: "fixture",
      updatedAt: at,
    });
    await setDoc(doc(db, `${season}/players/player-a`), {
      name: "Example Player",
      className: "Mage",
      role: "Damage",
      active: true,
      joinDate: "",
      leaveDate: "",
      version: 1,
      updatedBy: "fixture",
      updatedAt: at,
    });
    await setDoc(doc(db, `${season}/runs/baseline`), {
      status: "succeeded",
      requestedBy: "fixture",
      requestedAt: at,
      completedAt: at,
      inputVersions: captured,
    });
    await setDoc(doc(db, "publicSeasons/test-reports"), {
      latestRunId: "baseline",
    });
    await setDoc(doc(db, "bot/status"), { lastHeartbeatAt: Timestamp.now() });
  });
});
test.afterEach(async () => {
  releases.forEach((release) => release());
  await admin(async (ctx) => {
    const db = ctx.firestore();
    const requests = (
      await getDocs(collection(db, `${season}/runs`))
    ).docs.filter((d) => d.data().requestedBy !== "fixture");
    expect(requests).toHaveLength(expectedRequests);
    expect(
      (await getDoc(doc(db, `${season}/reports/${report}`))).data()?.version,
    ).toBe(expectedReportVersion);
    expect(
      (
        await getDoc(doc(db, `${season}/results/baseline/players/player-a`))
      ).data()?.name,
    ).toBe("Baseline Player");
  });
});
test.afterAll(async () => {
  await env?.cleanup();
});
test("retains the complete view during held reads and swaps rows, totals and nights together", async ({
  page,
}) => {
  await resultsPage(page);
  await page
    .getByRole("combobox", { name: "Result period" })
    .selectOption(nights.baseline);
  const held = await holdReads(page, "delayed");
  await seedResult("delayed", "Delayed Player", nights.delayed);
  await publish("delayed");
  await expect.poll(() => held.hits).toBeGreaterThan(0);
  await expect(status(page)).toContainText("Loading latest results");
  await expect(status(page)).not.toContainText("Results updated");
  await expect(
    page.getByText("Baseline Player", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("combobox", { name: "Result period" }),
  ).toHaveValue(nights.baseline);
  held.release();
  await expect(page.getByText("Delayed Player", { exact: true })).toBeVisible();
  await expect(page.getByText("Baseline Player", { exact: true })).toHaveCount(
    0,
  );
  await expect(
    page.getByRole("combobox", { name: "Result period" }),
  ).toHaveValue("season");
  await expect(
    page.getByRole("combobox", { name: "Result period" }).locator("option"),
  ).toHaveText(["Season to date", "Thursday, October 15"]);
  await expect(
    page.getByText("1 raid night this season", { exact: true }),
  ).toBeVisible();
  await expect(status(page)).toContainText("Updated");
});
test("public read denial preserves prior results and keyboard retry recovers on a narrow screen", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await resultsPage(page, false);
  await seedResult("staging", "Staging Player", nights.latest, false);
  await publish("staging");
  await expect(page.getByRole("alert")).toContainText(
    "Couldn’t load the latest results",
  );
  await expect(
    page.getByText("Baseline Player", { exact: true }),
  ).toBeVisible();
  await expect(page.getByText("Staging Player", { exact: true })).toHaveCount(
    0,
  );
  await expect(status(page)).toContainText("Latest results are unavailable");
  await admin(async (ctx) => {
    await setDoc(
      doc(ctx.firestore(), `${season}/results/staging`),
      { complete: true },
      { merge: true },
    );
  });
  await page.getByRole("button", { name: "Retry loading results" }).focus();
  await page.keyboard.press("Enter");
  await expect(page.getByText("Staging Player", { exact: true })).toBeVisible();
  await expect(page.getByRole("heading", { level: 1 })).toBeFocused();
  await expect(page.getByRole("alert")).toHaveCount(0);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});
test("officers reject incomplete output even with permission to read its children", async ({
  page,
}) => {
  await resultsPage(page);
  await seedResult("incomplete", "Incomplete Player", nights.latest, false);
  await publish("incomplete");
  await expect(page.getByRole("alert")).toContainText("Couldn’t load");
  await expect(
    page.getByText("Baseline Player", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("Incomplete Player", { exact: true }),
  ).toHaveCount(0);
  await admin(async (ctx) => {
    await setDoc(
      doc(ctx.firestore(), `${season}/results/incomplete`),
      { complete: true },
      { merge: true },
    );
  });
  await page.getByRole("button", { name: "Retry loading results" }).click();
  await expect(
    page.getByText("Incomplete Player", { exact: true }),
  ).toBeVisible();
});
test("a superseded delayed fetch never replaces the latest published view", async ({
  page,
}) => {
  await resultsPage(page);
  await page.evaluate(() => {
    const views: string[] = [];
    (window as unknown as { resultViews: string[] }).resultViews = views;
    new MutationObserver(() =>
      views.push(document.querySelector(".results-card")?.textContent ?? ""),
    ).observe(document.getElementById("root")!, {
      childList: true,
      subtree: true,
      characterData: true,
    });
  });
  const held = await holdReads(page, "delayed");
  await seedResult("delayed", "Delayed Player", nights.delayed);
  await publish("delayed");
  await expect.poll(() => held.hits).toBeGreaterThan(0);
  await seedResult("latest", "Latest Player", nights.latest, true, 3);
  await publish("latest");
  held.release();
  await expect(page.getByText("Latest Player", { exact: true })).toBeVisible();
  await expect(status(page)).toContainText("Updated");
  await expect(
    page.getByRole("combobox", { name: "Result period" }).locator("option"),
  ).toHaveText(["Season to date", "Friday, October 16"]);
  const views = await page.evaluate(
    () => (window as unknown as { resultViews: string[] }).resultViews,
  );
  expect(views.some((view) => view.includes("Delayed Player"))).toBe(false);
});
test("an edit saved during computation still needs another run after completion loads", async ({
  page,
}) => {
  await officer(page, env);
  expectedRequests = 1;
  await page.getByRole("button", { name: "Run Pebble", exact: true }).click();
  await expect(page.getByRole("button", { name: "Run queued" })).toBeDisabled();
  let id = "";
  await admin(async (ctx) => {
    id = (
      await getDocs(collection(ctx.firestore(), `${season}/runs`))
    ).docs.find((d) => d.id !== "baseline")!.id;
    await setDoc(
      doc(ctx.firestore(), `${season}/runs/${id}`),
      {
        status: "running",
        stage: "Calculating results",
        inputVersions: captured,
      },
      { merge: true },
    );
  });
  expectedReportVersion = 2;
  await page.getByRole("switch", { name: `Include ${report}` }).click();
  await page.getByRole("button", { name: "Save selection" }).click();
  await expect(
    page.getByRole("button", { name: "Save selection" }),
  ).toHaveCount(0);
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /^Results/ })
    .click();
  const held = await holdReads(page, id);
  await seedResult(id, "Completed Player", nights.latest);
  await publish(id);
  await expect.poll(() => held.hits).toBeGreaterThan(0);
  await expect(status(page)).toContainText("Loading latest results");
  await expect(
    page.getByText("Baseline Player", { exact: true }),
  ).toBeVisible();
  await page.getByText("Run history", { exact: true }).click();
  await expect(
    page.getByText("Computation completed", { exact: true }).first(),
  ).toBeVisible();
  await expect(status(page)).not.toContainText("Results updated");
  held.release();
  await expect(
    page.getByText("Completed Player", { exact: true }),
  ).toBeVisible();
  await expect(status(page)).toContainText("Changes ready for the next run");
  await expect(
    page.getByRole("button", { name: "Run Pebble", exact: true }),
  ).toBeEnabled();
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /^Reports/ })
    .click();
  await expect(
    page.getByRole("button", { name: "Run Pebble", exact: true }),
  ).toBeEnabled();
});

test("public first-load failure exposes retry without trapping the app on Connecting", async ({
  page,
}) => {
  await seedResult("first-load", "First Loaded Player", nights.latest, false);
  await publish("first-load");
  await page.goto("/");
  await expect(page.getByRole("alert")).toContainText(
    "Couldn’t load the latest results",
  );
  await expect(
    page.getByRole("button", { name: "Retry loading results" }),
  ).toBeEnabled();
  await expect(
    page.getByRole("heading", { name: "Connecting to Pebble…" }),
  ).toHaveCount(0);
  await admin(async (ctx) => {
    await setDoc(
      doc(ctx.firestore(), `${season}/results/first-load`),
      { complete: true },
      { merge: true },
    );
  });
  await page.getByRole("button", { name: "Retry loading results" }).click();
  await expect(
    page.getByText("First Loaded Player", { exact: true }),
  ).toBeVisible();
  await expect(page.getByRole("alert")).toHaveCount(0);
});

test("keeps officer warnings with displayed rows during loading and clears them with a clean result", async ({
  page,
}) => {
  await admin(async (ctx) => {
    await setDoc(
      doc(ctx.firestore(), `${season}/results/baseline`),
      {
        warnings: [
          "Baseline source warning",
          "2026-10-14: no Mythic encounters; no bench totals for this night",
        ],
      },
      { merge: true },
    );
  });
  await resultsPage(page);
  const region = page.getByRole("region", { name: "Result warnings" });
  await expect(
    region.getByRole("heading", { name: "Result warnings" }),
  ).toBeVisible();
  await expect(region.getByRole("listitem")).toHaveCount(2);
  const held = await holdReads(page, "delayed");
  await seedResult("delayed", "Delayed Player", nights.delayed, true, 2, [
    "New source warning",
  ]);
  await publish("delayed");
  await expect.poll(() => held.hits).toBeGreaterThan(0);
  await expect(status(page)).toContainText("Loading latest results");
  await expect(region).toContainText("Baseline source warning");
  await expect(
    page.getByText("New source warning", { exact: true }),
  ).toHaveCount(0);
  held.release();
  await expect(page.getByText("Delayed Player", { exact: true })).toBeVisible();
  await expect(region.getByRole("listitem")).toHaveText(["New source warning"]);
  await seedResult("latest", "Latest Player", nights.latest);
  await publish("latest");
  await expect(page.getByText("Latest Player", { exact: true })).toBeVisible();
  await expect(region).toHaveCount(0);
});

test("public warnings survive a failed load and change on keyboard retry at narrow width", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await admin(async (ctx) => {
    await setDoc(
      doc(ctx.firestore(), `${season}/results/baseline`),
      { warnings: ["Baseline source warning"] },
      { merge: true },
    );
  });
  await resultsPage(page, false);
  const region = page.getByRole("region", { name: "Result warnings" });
  await expect(region).toContainText("Baseline source warning");
  const warning =
    "2026-10-16: unknown characters: Example One, Example Two, Example Three, Example Four";
  await seedResult("staging", "Staging Player", nights.latest, false, 2, [
    warning,
  ]);
  await publish("staging");
  await expect(page.getByRole("alert")).toContainText(
    "Couldn’t load the latest results",
  );
  await expect(region).toContainText("Baseline source warning");
  await expect(page.getByText(warning, { exact: true })).toHaveCount(0);
  await expect(
    page.getByText("Baseline Player", { exact: true }),
  ).toBeVisible();
  await admin(async (ctx) => {
    await setDoc(
      doc(ctx.firestore(), `${season}/results/staging`),
      { complete: true },
      { merge: true },
    );
  });
  await page.getByRole("button", { name: "Retry loading results" }).focus();
  await page.keyboard.press("Enter");
  await expect(page.getByText("Staging Player", { exact: true })).toBeVisible();
  await expect(region.getByRole("listitem")).toHaveText([warning]);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});

async function reportNight(night: string) {
  await admin(async (ctx) => {
    await setDoc(
      doc(ctx.firestore(), `${season}/runs/baseline`),
      {
        reportSummaries: {
          [report]: {
            night,
            title: "Example report",
            checkedAt: Timestamp.now(),
          },
        },
      },
      { merge: true },
    );
  });
}
const raidDate = (page: Page) => page.getByLabel("Choose another raid date");
const raidPicker = (page: Page) =>
  page.getByRole("combobox", { name: "Raid night", exact: true });
async function overrides(page: Page) {
  await officer(page, env);
  await page
    .getByRole("navigation")
    .getByRole("button", { name: "Overrides", exact: true })
    .click();
}

test("defaults to report-only dates, follows late metadata and retains explicit dates and drafts", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), "publicSeasons/test-reports"), {});
  });
  await reportNight("2026-10-15");
  await overrides(page);
  await expect(raidDate(page)).toHaveValue("2026-10-15");
  await reportNight("2026-10-16");
  await expect(raidDate(page)).toHaveValue("2026-10-16");
  const notes = page.getByRole("textbox", { name: "Availability notes" });
  await notes.fill("Keep this night draft");
  await reportNight("2026-10-17");
  await expect(raidPicker(page).locator("option").first()).toHaveAttribute(
    "value",
    "2026-10-17",
  );
  await expect(raidDate(page)).toHaveValue("2026-10-16");
  await expect(notes).toHaveValue("Keep this night draft");
  await raidPicker(page).selectOption("2026-10-17");
  await expect(page.getByRole("dialog")).toBeVisible();
  await page.getByRole("button", { name: "Keep editing" }).focus();
  await page.keyboard.press("Enter");
  await expect(raidDate(page)).toHaveValue("2026-10-16");
  await expect(notes).toHaveValue("Keep this night draft");
  await raidPicker(page).selectOption("2026-10-17");
  await page.getByRole("button", { name: "Discard edits" }).click();
  await expect(raidDate(page)).toHaveValue("2026-10-17");
  await expect(notes).toHaveValue("");
  await raidDate(page).fill("2026-10-12");
  await reportNight("2026-10-18");
  await expect(raidPicker(page).locator("option").first()).toHaveAttribute(
    "value",
    "2026-10-18",
  );
  await expect(raidDate(page)).toHaveValue("2026-10-12");
  await expect(raidPicker(page)).toHaveValue("2026-10-12");
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});

test("keeps the default night during a held save and preserves its acknowledged context", async ({
  page,
}) => {
  await overrides(page);
  await expect(raidDate(page)).toHaveValue(nights.baseline);
  const notes = page.getByRole("textbox", { name: "Availability notes" });
  await notes.fill("Saved for the original night");
  await reportNight(nights.latest);
  await expect(raidPicker(page).locator("option").first()).toHaveAttribute(
    "value",
    nights.latest,
  );
  let release!: () => void, intercept!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const requested = new Promise<void>((resolve) => {
    intercept = resolve;
  });
  releases.push(release);
  await page.route(/\/documents:commit(?:\?|$)/, async (route) => {
    intercept();
    await gate;
    await route.continue();
  });
  await page
    .getByRole("button", { name: "Save adjustment", exact: true })
    .click();
  await requested;
  // The SDK may defer incoming snapshots until this held commit completes.
  await reportNight("2026-10-17");
  await expect(raidDate(page)).toHaveValue(nights.baseline);
  await raidPicker(page).selectOption(nights.latest);
  await expect(
    page.getByText("Wait for the save to finish.", { exact: true }),
  ).toBeVisible();
  await expect(raidDate(page)).toHaveValue(nights.baseline);
  release();
  await expect(
    page.getByRole("status", { name: "Availability changes" }),
  ).toContainText("Availability saved");
  await expect(raidPicker(page).locator("option").first()).toHaveAttribute(
    "value",
    "2026-10-17",
  );
  await expect(raidDate(page)).toHaveValue(nights.baseline);
  await admin(async (ctx) => {
    const overrides = await getDocs(
      collection(ctx.firestore(), `${season}/availabilityOverrides`),
    );
    expect(overrides.docs.map((d) => d.id)).toEqual([
      `${nights.baseline}_player-a`,
    ]);
    expect(overrides.docs[0].data().notes).toBe("Saved for the original night");
  });
});

test("explains non-officer access through grant, revocation and keyboard sign-out", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const uid = await signIn(page);
  const access = page.getByRole("region", {
    name: "Officer access not enabled",
  });
  await expect(access).toContainText("You can still read completed results");
  await expect(access).toContainText(
    "Editing inputs and running Pebble require an enabled officer account",
  );
  await expect(
    page.getByText("Baseline Player", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("navigation").getByRole("button", { name: /^Reports/ }),
  ).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Run Pebble", exact: true }),
  ).toHaveCount(0);
  await expect(access.locator("code")).toHaveText(uid);
  const details = access.getByText("Account details", { exact: true });
  await details.focus();
  await page.keyboard.press("Enter");
  await expect(access.locator("code")).toBeHidden();
  await page.keyboard.press("Enter");
  await expect(access.locator("code")).toBeVisible();
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), `officers/${uid}`), { enabled: true });
  });
  await expect(access).toHaveCount(0);
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /^Reports/ })
    .click();
  await expect(
    page.getByRole("textbox", { name: "Warcraft Logs reports" }),
  ).toBeVisible();
  await page
    .getByRole("textbox", { name: "Warcraft Logs reports" })
    .fill("Unsaved private draft");
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), `officers/${uid}`), { enabled: false });
  });
  await expect(access).toBeVisible();
  await expect(access.locator("code")).toBeHidden();
  await expect(
    page.getByRole("textbox", { name: "Warcraft Logs reports" }),
  ).toHaveCount(0);
  await expect(
    page.getByRole("navigation").getByRole("button", { name: /^Overrides/ }),
  ).toHaveCount(0);
  await expect(
    page.getByText("Baseline Player", { exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Sign out", exact: true }).focus();
  await page.keyboard.press("Enter");
  await expect(access).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Officer sign-in" }),
  ).toBeVisible();
  await expect(
    page.getByText("Baseline Player", { exact: true }),
  ).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});

test("public empty results distinguish no completed run from a completed period with no player time", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), "publicSeasons/test-reports"), {});
  });
  await page.goto("/");
  await expect(page.getByText(/No completed results yet/)).toContainText(
    "after an officer runs Pebble",
  );
  await expect(
    page.getByRole("button", { name: "Manage reports" }),
  ).toHaveCount(0);
  await expect(
    page.getByRole("heading", { name: "No player time for this period" }),
  ).toHaveCount(0);
  await seedResult("empty", "Empty Player", nights.latest);
  await admin(async (ctx) => {
    await deleteDoc(
      doc(ctx.firestore(), `${season}/results/empty/players/player-a`),
    );
    await setDoc(
      doc(ctx.firestore(), `${season}/results/empty/nights/${nights.latest}`),
      { players: [] },
      { merge: true },
    );
  });
  await publish("empty");
  await expect(
    page.getByRole("heading", { name: "No player time for this period" }),
  ).toBeVisible();
  await expect(page.getByText(/No completed results yet/)).toHaveCount(0);
  await expect(
    page.getByText(
      /No player time was recorded for this period in the completed results/,
    ),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Manage reports" }),
  ).toHaveCount(0);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});

test("officer empty periods and recent history keep progress visible and link to Reports without writes", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await admin(async (ctx) => {
    const db = ctx.firestore();
    await setDoc(
      doc(db, `${season}/results/baseline/nights/${nights.latest}`),
      {
        night: nights.latest,
        players: [],
        duration: 0,
        reports: 0,
        timing: { start: "", breakStart: "", breakEnd: "", end: "", notes: "" },
      },
    );
    await deleteDoc(doc(db, `${season}/runs/baseline`));
    await setDoc(doc(db, `${season}/runs/active-fixture`), {
      status: "queued",
      requestedBy: "fixture",
      requestedAt: Timestamp.now(),
    });
  });
  await resultsPage(page);
  await page
    .getByRole("combobox", { name: "Result period" })
    .selectOption(nights.latest);
  await expect(
    page.getByRole("heading", { name: "No player time for this period" }),
  ).toBeVisible();
  await expect(
    page.getByText(/Use Reports in the sidebar to review the included logs/),
  ).toBeVisible();
  await expect(page.getByRole("button", { name: "Run queued" })).toBeDisabled();
  const history = page.getByText("Run history", { exact: true });
  await history.click();
  await expect(
    page.getByText("No completed runs in recent history.", { exact: true }),
  ).toBeVisible();
  await page
    .getByRole("combobox", { name: "Result period" })
    .selectOption("season");
  await expect(
    page.getByText("Baseline Player", { exact: true }),
  ).toBeVisible();
  await page
    .getByRole("combobox", { name: "Result period" })
    .selectOption(nights.latest);
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /^Reports/ })
    .focus();
  await page.keyboard.press("Enter");
  await expect(
    page.getByRole("textbox", { name: "Warcraft Logs reports" }),
  ).toBeVisible();
  await expect(
    page.getByRole("textbox", { name: "Warcraft Logs reports" }),
  ).toHaveValue("");
  await expect(page.getByRole("button", { name: "Run queued" })).toBeDisabled();
  await admin(async (ctx) => {
    const saved = (
      await getDoc(doc(ctx.firestore(), `${season}/reports/${report}`))
    ).data()!;
    expect(saved.enabled).toBe(true);
    expect(saved.version).toBe(1);
    expect(
      (await getDocs(collection(ctx.firestore(), `${season}/runs`))).docs.map(
        (d) => d.id,
      ),
    ).toEqual(["active-fixture"]);
  });
});

async function sourceMetadata(
  id: string,
  summaries: Record<string, { title: string }>,
) {
  await admin(async (ctx) => {
    await setDoc(
      doc(ctx.firestore(), `${season}/runs/${id}`),
      {
        reportSummaries: Object.fromEntries(
          Object.entries(summaries).map(([code, summary]) => [
            code,
            { ...summary, night: nights.baseline, checkedAt: Timestamp.now() },
          ]),
        ),
        requestedBy: "fixture",
      },
      { merge: true },
    );
  });
}

test("officer warning sources stay with displayed results instead of newer history and replace together", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await sourceMetadata("baseline", {
    [report]: { title: "Original source report" },
  });
  await admin(async (ctx) => {
    await setDoc(
      doc(ctx.firestore(), `${season}/results/baseline`),
      { warnings: ["Baseline warning"] },
      { merge: true },
    );
  });
  await resultsPage(page);
  const sources = page.getByRole("region", {
    name: "Source reports for this run",
  });
  const original = sources.getByRole("link", {
    name: "Original source report",
  });
  await expect(original).toHaveAttribute(
    "href",
    `https://www.warcraftlogs.com/reports/${report}`,
  );
  await original.focus();
  await expect(original).toBeFocused();
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), `${season}/runs/other-metadata`), {
      status: "failed",
      requestedBy: "fixture",
      requestedAt: Timestamp.now(),
      reportSummaries: {
        [report]: {
          title: "Unpublished report title",
          night: nights.baseline,
          checkedAt: Timestamp.now(),
        },
      },
    });
  });
  await expect(original).toBeVisible();
  const held = await holdReads(page, "delayed");
  await seedResult("delayed", "Delayed Player", nights.delayed, true, 2, [
    "New warning",
  ]);
  await sourceMetadata("delayed", { BBBBBBBBBBBBBBBB: { title: "  " } });
  await publish("delayed");
  await expect.poll(() => held.hits).toBeGreaterThan(0);
  await expect(original).toBeVisible();
  await expect(
    sources.getByRole("link", { name: "BBBBBBBBBBBBBBBB" }),
  ).toHaveCount(0);
  held.release();
  await expect(page.getByText("Delayed Player", { exact: true })).toBeVisible();
  await expect(
    sources.getByRole("link", { name: "BBBBBBBBBBBBBBBB" }),
  ).toHaveAttribute(
    "href",
    "https://www.warcraftlogs.com/reports/BBBBBBBBBBBBBBBB",
  );
  await expect(original).toHaveCount(0);
  await expect(
    page.getByText("Unpublished report title", { exact: true }),
  ).toHaveCount(0);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  await seedResult("latest", "Latest Player", nights.latest, true, 2, [
    "Missing source metadata warning",
  ]);
  await publish("latest");
  await expect(page.getByText("Latest Player", { exact: true })).toBeVisible();
  await expect(
    page.getByText("Missing source metadata warning", { exact: true }),
  ).toBeVisible();
  await expect(sources).toHaveCount(0);
});

test("public warnings do not expose private source metadata", async ({
  page,
}) => {
  await sourceMetadata("baseline", {
    [report]: { title: "Officer source metadata" },
  });
  await admin(async (ctx) => {
    await setDoc(
      doc(ctx.firestore(), `${season}/results/baseline`),
      { warnings: ["Public result warning"] },
      { merge: true },
    );
  });
  await resultsPage(page, false);
  await expect(
    page.getByText("Public result warning", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("region", { name: "Source reports for this run" }),
  ).toHaveCount(0);
  await expect(
    page.getByText("Officer source metadata", { exact: true }),
  ).toHaveCount(0);
});

test("keyboard details, validation and labelled discard work across narrow workflow pages", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await resultsPage(page);
  const noOverflow = async () =>
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    ).toBe(true);
  const details = page.getByRole("button", {
    name: "Details for Baseline Player",
  });
  const target = await details.getAttribute("aria-controls");
  expect(target).toBeTruthy();
  const breakdown = page.getByRole("region", {
    name: "Availability breakdown for Baseline Player",
  });
  await expect(breakdown).toBeHidden();
  await details.focus();
  await page.keyboard.press("Space");
  await expect(breakdown).toBeVisible();
  await expect(breakdown).toHaveAttribute("id", target!);
  await noOverflow();
  await page.keyboard.press("Enter");
  await expect(breakdown).toBeHidden();
  await expect(details).toBeFocused();
  await page.keyboard.press("Enter");
  await page.getByRole("button", { name: "Edit availability" }).click();
  await page
    .getByRole("combobox", { name: "Before break", exact: true })
    .selectOption("partial");
  const minutes = page.getByRole("spinbutton", {
    name: "Before break available minutes",
  });
  await minutes.fill("0");
  await expect(minutes).toHaveAttribute("aria-invalid", "true");
  await expect(minutes).toHaveAttribute(
    "aria-describedby",
    /availability-validation/,
  );
  await expect(page.locator("#availability-validation")).toBeVisible();
  await noOverflow();
  await page.getByRole("tab", { name: "Raid timing" }).click();
  const dialog = page.getByRole("dialog", { name: "Keep your changes?" });
  await expect(dialog).toBeVisible();
  await expect(dialog).toHaveAttribute(
    "aria-describedby",
    "discard-description",
  );
  await expect(
    page.getByRole("button", { name: "Keep editing" }),
  ).toBeFocused();
  await page.keyboard.press("Escape");
  await expect(dialog).toBeHidden();
  await expect(minutes).toHaveValue("0");
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  await page.getByRole("tab", { name: "Raid timing" }).click();
  await expect(
    page.getByRole("tabpanel", { name: "Raid timing" }),
  ).toBeVisible();
  await noOverflow();
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /^Reports/ })
    .click();
  const reportInput = page.getByRole("textbox", {
    name: "Warcraft Logs reports",
  });
  await reportInput.fill("invalid");
  await expect(reportInput).toHaveAttribute(
    "aria-describedby",
    /report-validation/,
  );
  await expect(page.locator("#report-validation")).toContainText("invalid");
  await noOverflow();
  const runStatus = status(page);
  await expect(runStatus).toHaveAttribute("aria-atomic", "true");
  await expect(runStatus.getByRole("button")).toHaveCount(0);
});

test("officer-access retry recovers heading focus after a denied membership listener", async ({
  page,
}) => {
  const original = readFileSync(
    new URL("../../../firebase/firestore.rules", import.meta.url),
    "utf8",
  );
  await resultsPage(page);
  await page
    .getByRole("combobox", { name: "Result period" })
    .selectOption(nights.baseline);
  await page
    .getByRole("button", { name: "Details for Baseline Player" })
    .click();
  const selectedURL = page.url();
  const uid = await adminUid();
  const denied = original.replace(
    "allow get: if request.auth != null && uid == request.auth.uid;",
    "allow get: if false;",
  );
  expect(denied).not.toBe(original);
  try {
    await setDemoRules(denied);
    await admin(async (ctx) => {
      await setDoc(doc(ctx.firestore(), `officers/${uid}`), {
        enabled: true,
        revision: 1,
      });
    });
    await expect(
      page.getByRole("button", { name: "Retry officer access" }),
    ).toBeVisible();
    await expect(
      page.getByText("Baseline Player", { exact: true }),
    ).toBeVisible();
    await setDemoRules(original);
    await page.getByRole("button", { name: "Retry officer access" }).focus();
    await page.keyboard.press("Enter");
    await expect(page.getByRole("heading", { level: 1 })).toBeFocused();
    await expect(
      page.getByRole("button", { name: "Manage reports" }),
    ).toHaveCount(0);
    await expect(
      page.getByRole("navigation").getByRole("button", { name: /^Reports/ }),
    ).toBeVisible();
    await expect(
      page.getByRole("combobox", { name: "Result period" }),
    ).toHaveValue(nights.baseline);
    await expect(
      page.getByRole("region", {
        name: "Availability breakdown for Baseline Player",
      }),
    ).toBeVisible();
    expect(page.url()).toBe(selectedURL);
    await expect(
      page.getByRole("button", { name: "Retry officer access" }),
    ).toHaveCount(0);
  } finally {
    await setDemoRules(original);
  }
});

async function adminUid() {
  let uid = "";
  await admin(async (ctx) => {
    const officers = await getDocs(collection(ctx.firestore(), "officers"));
    uid = officers.docs.find((d) => d.data().enabled)?.id ?? "";
  });
  expect(uid).not.toBe("");
  return uid;
}

async function setDemoRules(rules: string) {
  const temporary = await initializeTestEnvironment({
    projectId: "demo-pebble-reports",
    firestore: { host: "127.0.0.1", port: 8180, rules },
  });
  await temporary.cleanup();
}

for (const signedIn of [false, true]) {
  test(`only season and night periods are selectable for ${signedIn ? "officers" : "public readers"}`, async ({
    page,
  }) => {
    await admin(async (ctx) => {
      await setDoc(
        doc(ctx.firestore(), `${season}/results/baseline/weeks/2026-10-13`),
        { players: [] },
      );
    });
    await page.setViewportSize({ width: 390, height: 844 });
    await resultsPage(page, signedIn);
    const period = page.getByRole("combobox", { name: "Result period" });
    await expect(period.locator("option")).toHaveText([
      "Season to date",
      "Wednesday, October 14",
    ]);
    await period.selectOption(nights.baseline);
    await expect(
      page.getByText("Baseline Player", { exact: true }),
    ).toBeVisible();
    const held = await holdReads(page, "night-new");
    await seedResult("night-new", "Latest Player", nights.latest);
    await publish("night-new");
    await expect.poll(() => held.hits).toBeGreaterThan(0);
    await expect(period).toHaveValue(nights.baseline);
    await expect(status(page)).toContainText("Loading latest results");
    held.release();
    await expect(period).toHaveValue("season");
    await expect(
      page.getByText("Latest Player", { exact: true }),
    ).toBeVisible();
    await expect(period.locator("option")).toHaveText([
      "Season to date",
      "Friday, October 16",
    ]);
    await expect(
      page.getByRole("button", { name: "Run Pebble", exact: true }),
    ).toHaveCount(signedIn ? 1 : 0);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    ).toBe(true);
  });
}

for (const signedIn of [false, true]) {
  test(`recorded played breakdowns follow result periods for ${signedIn ? "officers" : "public readers"}`, async ({
    page,
  }) => {
    await admin(async (ctx) => {
      const db = ctx.firestore();
      const playerRef = doc(db, `${season}/results/baseline/players/player-a`);
      const nightRef = doc(
        db,
        `${season}/results/baseline/nights/${nights.baseline}`,
      );
      const nightData = (await getDoc(nightRef)).data()!;
      await setDoc(
        playerRef,
        { prePlayed: 13, postPlayed: 47 },
        { merge: true },
      );
      await setDoc(
        nightRef,
        {
          players: [
            {
              ...nightData.players[0],
              prePlayed: 0,
              postPlayed: 60,
              overridden: true,
            },
          ],
        },
        { merge: true },
      );
    });
    await page.setViewportSize({ width: 390, height: 844 });
    await resultsPage(page, signedIn);
    const details = page.getByRole("button", {
      name: "Details for Baseline Player",
    });
    const breakdown = page.getByRole("region", {
      name: "Availability breakdown for Baseline Player",
    });
    await details.focus();
    await page.keyboard.press("Enter");
    await expect(
      breakdown.getByRole("table", { name: "Time by break" }).getByRole("row"),
    ).toHaveText([
      "PeriodBenchPlayed",
      "Before break10 min13 min",
      "After break20 min47 min",
    ]);
    await expect(
      breakdown.getByRole("button", { name: "Edit availability" }),
    ).toHaveCount(signedIn ? 1 : 0);
    await page
      .getByRole("combobox", { name: "Result period" })
      .selectOption(nights.baseline);
    await expect(breakdown).toHaveCount(0);
    await details.focus();
    await page.keyboard.press("Enter");
    await expect(
      breakdown.getByRole("table", { name: "Time by break" }).getByRole("row"),
    ).toHaveText([
      "PeriodBenchPlayed",
      "Before break10 min0 min",
      "After break20 min60 min",
    ]);
    await breakdown
      .getByText("Availability explanation", { exact: true })
      .click();
    await expect(breakdown).toContainText("Officer adjustment applied");
    await expect(breakdown).not.toContainText("undefined");
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    ).toBe(true);
  });
}

test("public result links survive delayed reads, refresh, copy and back/forward", async ({
  page,
  context,
}) => {
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  await admin(async (ctx) => {
    await setDoc(
      doc(ctx.firestore(), `${season}/results/baseline/weeks/2026-10-13`),
      {
        players: [
          {
            game_week: "2026-10-13",
            main: "Baseline Player",
            played_min: 123,
            bench_min: 47,
            bench_pre_min: 20,
            bench_post_min: 27,
          },
        ],
      },
    );
  });
  const held = await holdReads(page, "baseline");
  const linked = `/?view=results&period=${nights.baseline}&player=player-a`;
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(linked, { waitUntil: "domcontentloaded" });
  await expect.poll(() => held.hits).toBeGreaterThan(0);
  expect(new URL(page.url()).searchParams.get("period")).toBe(nights.baseline);
  expect(new URL(page.url()).searchParams.get("player")).toBe("player-a");
  held.release();
  const period = page.getByRole("combobox", { name: "Result period" });
  const breakdown = page.getByRole("region", {
    name: "Availability breakdown for Baseline Player",
  });
  await expect(period).toHaveValue(nights.baseline);
  await expect(breakdown).toBeVisible();
  await page.reload();
  await expect(period).toHaveValue(nights.baseline);
  await expect(breakdown).toBeVisible();
  await page.getByRole("button", { name: "Copy result link" }).focus();
  await page.keyboard.press("Enter");
  await expect(
    page.getByText("Result link copied.", { exact: true }),
  ).toBeVisible();
  const copied = new URL(
    await page.evaluate(() => navigator.clipboard.readText()),
  );
  expect([...copied.searchParams.keys()]).toEqual(["view", "period", "player"]);
  expect(copied.searchParams.get("player")).toBe("player-a");
  await period.selectOption(nights.baseline);
  await page
    .getByRole("button", { name: "Details for Baseline Player" })
    .click();
  expect(new URL(page.url()).searchParams.get("period")).toBe(nights.baseline);
  await page.reload();
  await expect(period).toHaveValue(nights.baseline);
  await expect(breakdown).toBeVisible();
  await period.selectOption("season");
  await page.evaluate(() => history.back());
  await expect(period).toHaveValue(nights.baseline);
  await expect(breakdown).toBeVisible();
  await page.evaluate(() => history.forward());
  await expect(period).toHaveValue("season");
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});

test("missing public result targets explain safe fallbacks", async ({
  page,
}) => {
  await page.goto("/?view=results&period=2020-01-01&player=missing");
  await expect(
    page.getByText(/This result period is not available/),
  ).toBeVisible();
  await expect(
    page.getByRole("combobox", { name: "Result period" }),
  ).toHaveValue("season");
  expect(new URL(page.url()).searchParams.has("player")).toBe(false);
  await page.goto(`/?view=results&period=${nights.baseline}&player=missing`);
  await expect(page.getByText(/This player is not available/)).toBeVisible();
  await expect(
    page.getByRole("combobox", { name: "Result period" }),
  ).toHaveValue(nights.baseline);
  expect(new URL(page.url()).searchParams.has("player")).toBe(false);
});

test("browser Back preserves officer drafts until discard and blocks pending saves", async ({
  page,
}) => {
  await resultsPage(page);
  await page
    .getByRole("combobox", { name: "Result period" })
    .selectOption(nights.baseline);
  const reportsNav = () =>
    page.getByRole("navigation").getByRole("button", { name: /^Reports/ });
  await reportsNav().click();
  const input = page.getByRole("textbox", { name: "Warcraft Logs reports" });
  await input.fill("Unsaved draft");
  const before = page.url();
  await page.evaluate(() => history.back());
  const dialog = page.getByRole("dialog", { name: "Keep your changes?" });
  await expect(dialog).toBeVisible();
  expect(page.url()).toBe(before);
  await dialog.getByRole("button", { name: "Keep editing" }).click();
  await expect(input).toHaveValue("Unsaved draft");
  await page.evaluate(() => history.back());
  await expect(dialog).toBeVisible();
  await dialog.getByRole("button", { name: "Discard edits" }).click();
  await expect(
    page.getByRole("combobox", { name: "Result period" }),
  ).toHaveValue(nights.baseline);
  await reportsNav().click();
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  releases.push(release);
  await page.route(/\/documents:commit(?:\?|$)/, async (route) => {
    await gate;
    await route.continue();
  });
  await input.fill("BBBBBBBBBBBBBBBB");
  await page.getByRole("button", { name: "Add report", exact: true }).click();
  await expect(
    page.getByRole("status", { name: "Report changes" }),
  ).toContainText("Saving");
  await page.evaluate(() => history.back());
  await expect(
    page.getByText("Wait for the save to finish.", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("textbox", { name: "Warcraft Logs reports" }),
  ).toBeVisible();
  expect(new URL(page.url()).searchParams.has("view")).toBe(false);
  release();
  await expect(
    page.getByRole("status", { name: "Report changes" }),
  ).toContainText("Reports added");
  await page.evaluate(() => history.back());
  await expect(
    page.getByRole("combobox", { name: "Result period" }),
  ).toHaveValue(nights.baseline);
});

test("recorded season rankings preserve eligibility, zero rows, tie order and undefined ratios", async ({
  page,
}) => {
  await admin(async (ctx) => {
    const db = ctx.firestore();
    const base = (
      await getDoc(doc(db, `${season}/results/baseline/players/player-a`))
    ).data()!;
    // Season exclusion is explicit; the same recorded historical main remains
    // present on its night/week rather than being filtered by current roster state.
    await setDoc(
      doc(db, `${season}/results/baseline/players/player-a`),
      { ranking: null },
      { merge: true },
    );
    const rows = [
      { id: "zero", name: "Zero", played: 0, bench: 0, rank: 1, ratio: null },
      {
        id: "upper",
        name: "Zulu",
        played: 100,
        bench: 20,
        rank: 2,
        ratio: 0.2,
      },
      {
        id: "lower",
        name: "alpha",
        played: 50,
        bench: 20,
        rank: 3,
        ratio: 0.4,
      },
    ];
    for (const row of rows) {
      await setDoc(doc(db, `${season}/results/baseline/players/${row.id}`), {
        ...base,
        ...row,
        preBench: row.bench,
        postBench: 0,
        ranking: {
          rank: row.rank,
          played: row.played,
          bench: row.bench,
          ratio: row.ratio,
        },
      });
    }
    await setDoc(doc(db, `${season}/results/baseline/weeks/2026-10-13`), {
      players: [
        {
          game_week: "2026-10-13",
          main: "Baseline Player",
          played_min: 60,
          bench_min: 30,
          bench_pre_min: 10,
          bench_post_min: 20,
        },
      ],
    });
  });
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");
  await expect(page.locator(".result-name strong")).toHaveText([
    "Zero",
    "Zulu",
    "alpha",
  ]);
  await expect(page.locator("td.rank")).toHaveText(["01", "02", "03"]);
  const zero = page.getByRole("button", { name: "Details for Zero" });
  await zero.focus();
  await page.keyboard.press("Enter");
  await expect(
    page.getByRole("region", { name: "Availability breakdown for Zero" }),
  ).not.toContainText("Bench:played ratio");
  await page.getByRole("button", { name: "Details for Zulu" }).click();
  await expect(
    page.getByRole("region", { name: "Availability breakdown for Zulu" }),
  ).not.toContainText("Bench:played ratio");
  const period = page.getByRole("combobox", { name: "Result period" });
  await period.selectOption(nights.baseline);
  await expect(page.locator(".result-name strong")).toHaveText([
    "Baseline Player",
  ]);
  await page
    .getByRole("button", { name: "Details for Baseline Player" })
    .click();
  await expect(
    page.getByText("Bench:played ratio", { exact: false }),
  ).toHaveCount(0);
  await period.selectOption("season");
  await expect(page.locator(".result-name strong")).toHaveText([
    "Zero",
    "Zulu",
    "alpha",
  ]);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});
