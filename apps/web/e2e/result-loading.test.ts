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
    page.getByText("2 included reports", { exact: true }),
  ).toBeVisible();
  await expect(status(page)).toContainText("Results updated");
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
  await expect(status(page)).toContainText("Results updated");
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
    page.getByRole("heading", { name: "Add a report" }),
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
  await page
    .getByRole("button", { name: "Sign out to switch accounts" })
    .focus();
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
    page.getByText(/Use Manage reports to review the included logs/),
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
  await page.getByRole("button", { name: "Manage reports" }).focus();
  await page.keyboard.press("Enter");
  await expect(
    page.getByRole("heading", { name: "Add a report" }),
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
