import { expect, test, type Page } from "@playwright/test";
import {
  collection,
  doc,
  getDoc,
  getDocs,
  setDoc,
  Timestamp,
  writeBatch,
} from "firebase/firestore";
import { setupEmulator } from "./support/emulator";
import { seedHeartbeat } from "./support/fixtures";
import { officer } from "./support/officer";
const season = "seasons/test-reports",
  report = "AAAAAAAAAAAAAAAA";
const versions = { [`reports/${report}`]: 1, "players/player-a": 1 };
const emulator = setupEmulator();
let expectedRequests = 0;
const admin = emulator.admin;
const runStatus = (page: Page) =>
  page.getByRole("status", { name: "Run status" });
const runButton = (page: Page) =>
  page.getByRole("button", { name: "Run Pebble", exact: true });
async function requests() {
  let result: { id: string }[] = [];
  await admin(async (ctx) => {
    result = (
      await getDocs(collection(ctx.firestore(), `${season}/runs`))
    ).docs.filter((d) => d.id !== "baseline" && !d.id.startsWith("history-"));
  });
  return result;
}
async function setRun(id: string, value: Record<string, unknown>) {
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), `${season}/runs/${id}`), value, {
      merge: true,
    });
  });
}
async function histories() {
  await admin(async (ctx) => {
    const db = ctx.firestore(),
      batch = writeBatch(db);
    for (let i = 0; i < 21; i++)
      batch.set(doc(db, `${season}/runs/history-${i}`), {
        status: "failed",
        requestedBy: "fixture",
        requestedAt: Timestamp.fromMillis(Date.now() + 60000 + i),
        completedAt: Timestamp.now(),
        error: "Other synthetic request",
      });
    await batch.commit();
  });
}
function capture(page: Page) {
  const lookups: string[] = [],
    creates: string[] = [];
  page.on("request", (request) => {
    if (!request.url().includes("127.0.0.1:8180/v1/")) return;
    if (request.method() !== "POST" || !request.postData()) return;
    const body = request.postDataJSON();
    if (request.url().includes("documents:batchGet"))
      for (const path of body.documents ?? [])
        if (path.includes("/runs/")) lookups.push(path.split("/").at(-1));
    if (request.url().includes("documents:commit"))
      for (const write of body.writes ?? [])
        if (write.update?.name?.includes("/runs/"))
          creates.push(write.update.name.split("/").at(-1));
  });
  return { lookups, creates };
}
async function previousResults(page: Page) {
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /^Results/ })
    .click();
  await expect(page.getByText("Example Player", { exact: true })).toBeVisible();
}
test.beforeEach(async () => {
  expectedRequests = 0;
  await admin(async (ctx) => {
    const db = ctx.firestore(),
      completedAt = Timestamp.fromMillis(Date.now() - 3600000);
    await setDoc(doc(db, `${season}/reports/${report}`), {
      enabled: true,
      notes: "Baseline report",
      version: 1,
      updatedBy: "fixture",
      updatedAt: completedAt,
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
      updatedAt: completedAt,
    });
    await setDoc(doc(db, `${season}/results/baseline`), {
      complete: true,
      completedAt,
      reportCount: 1,
    });
    await setDoc(doc(db, `${season}/results/baseline/players/player-a`), {
      id: "player-a",
      name: "Example Player",
      className: "Mage",
      role: "Damage",
      color: "#b9c7d8",
      played: 90,
      bench: 30,
      preBench: 10,
      postBench: 20,
      overridden: false,
      nights: 1,
    });
    await setDoc(doc(db, `${season}/runs/baseline`), {
      status: "succeeded",
      requestedBy: "fixture",
      requestedAt: completedAt,
      completedAt,
      inputVersions: versions,
    });
    await setDoc(doc(db, "publicSeasons/test-reports"), {
      latestRunId: "baseline",
    });
    await setDoc(doc(db, "bot/status"), {
      lastHeartbeatAt: Timestamp.fromMillis(Date.now() - 300000),
    });
  });
});
test.afterEach(async () => {
  expect(await requests()).toHaveLength(expectedRequests);
  await admin(async (ctx) => {
    const db = ctx.firestore();
    expect(
      (await getDoc(doc(db, "publicSeasons/test-reports"))).data()?.latestRunId,
    ).toBe("baseline");
    expect(
      (await getDoc(doc(db, `${season}/reports/${report}`))).data()?.version,
    ).toBe(1);
    expect(
      (await getDoc(doc(db, `${season}/players/player-a`))).data()?.version,
    ).toBe(1);
  });
});
test("coalesces held clicks, announces offline queue/stages, and preserves results through failure", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await officer(page, emulator.environment);
  await page
    .getByRole("textbox", { name: "Warcraft Logs reports" })
    .fill("Unsaved draft");
  await expect(runButton(page)).toBeDisabled();
  expect(await requests()).toHaveLength(0);
  await page.getByRole("textbox", { name: "Warcraft Logs reports" }).fill("");
  let release!: () => void, intercept!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const requested = new Promise<void>((resolve) => {
    intercept = resolve;
  });
  const calls = capture(page);
  await page.route(/\/documents:batchGet(?:\?|$)/, async (route) => {
    intercept();
    await gate;
    await route.continue();
  });
  await runButton(page).dblclick();
  await requested;
  await expect(
    page.getByRole("button", { name: "Submitting run…" }),
  ).toBeDisabled();
  await expect(runStatus(page)).toContainText(
    "Waiting for the server to confirm",
  );
  await expect(runStatus(page).locator("..")).toHaveClass(/is-running/);
  expect(await requests()).toHaveLength(0);
  expectedRequests = 1;
  release();
  await expect(
    page.getByRole("button", { name: "Run queued", exact: true }),
  ).toBeDisabled();
  await expect(runStatus(page)).toContainText(
    "Queued · waiting for your local bot",
  );
  expect(calls.creates).toHaveLength(1);
  expect(new Set(calls.lookups).size).toBe(1);
  const id = (await requests())[0].id;
  await previousResults(page);
  await admin(async (ctx) => {
    await seedHeartbeat(ctx.firestore());
  });
  for (const stage of [
    "Validating inputs",
    "Fetching reports",
    "Calculating results",
    "Updating results",
  ]) {
    await setRun(id, { status: "running", stage });
    await expect(runStatus(page)).toContainText(`${stage}…`);
    await expect(
      page.getByRole("button", { name: "Running Pebble", exact: true }),
    ).toBeDisabled();
  }
  await setRun(id, {
    status: "failed",
    completedAt: Timestamp.now(),
    error: "Synthetic source failure",
  });
  await expect(runStatus(page)).toContainText("Last run didn’t finish");
  await expect(page.getByText("Example Player", { exact: true })).toBeVisible();
  await page.getByText("Run history", { exact: true }).click();
  await expect(
    page.getByText("Synthetic source failure", { exact: true }),
  ).toBeVisible();
  await expect(runButton(page)).toBeEnabled();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  await page.unroute(/\/documents:batchGet(?:\?|$)/);
  expectedRequests = 2;
  await runButton(page).focus();
  await page.keyboard.press("Enter");
  await expect(
    page.getByRole("button", { name: "Run queued", exact: true }),
  ).toBeDisabled();
  expect(new Set(calls.creates).size).toBe(2);
});
test("retains the request ID after a denied submission and creates one request on retry", async ({
  page,
}) => {
  await officer(page, emulator.environment);
  const calls = capture(page);
  await page.route(/\/documents:commit(?:\?|$)/, (route) =>
    route.fulfill({
      status: 403,
      contentType: "application/json",
      body: JSON.stringify({
        error: {
          code: 403,
          status: "PERMISSION_DENIED",
          message: "Test run submission denied",
        },
      }),
    }),
  );
  await runButton(page).click();
  await expect(page.getByRole("alert")).toContainText(/denied|permissions/i);
  await expect(runButton(page)).toBeEnabled();
  expect(await requests()).toHaveLength(0);
  await page.unroute(/\/documents:commit(?:\?|$)/);
  expectedRequests = 1;
  await runButton(page).click();
  await expect(
    page.getByRole("button", { name: "Run queued", exact: true }),
  ).toBeDisabled();
  expect(calls.creates).toHaveLength(2);
  expect(new Set(calls.creates).size).toBe(1);
  expect(new Set(calls.lookups).size).toBe(1);
});
test("observes an accepted request after a lost acknowledgement without resubmission", async ({
  page,
}) => {
  await officer(page, emulator.environment);
  const calls = capture(page);
  let accepted = "";
  await page.route(/\/documents:commit(?:\?|$)/, async (route) => {
    const body = route.request().postDataJSON();
    accepted = body.writes
      .find((write: { update?: { name: string } }) => write.update)
      ?.update.name.split("/")
      .at(-1);
    const response = await route.fetch();
    expect(response.status()).toBe(200);
    // The real emulator committed, but the browser receives an error instead of
    // acknowledgement. Roll recent history forward before delivering the error.
    await setRun(accepted, { fixtureMarker: "accepted before response loss" });
    await histories();
    await route.fulfill({
      status: 403,
      contentType: "application/json",
      body: JSON.stringify({
        error: {
          code: 403,
          status: "PERMISSION_DENIED",
          message: "Submission acknowledgement unavailable",
        },
      }),
    });
  });
  expectedRequests = 1;
  await runButton(page).click();
  await expect(page.getByRole("alert")).toContainText(
    "Submission acknowledgement unavailable",
  );
  expect((await requests())[0].id).toBe(accepted);
  await page.unroute(/\/documents:commit(?:\?|$)/);
  // The independent queue listener discovers the accepted request automatically.
  await expect(
    page.getByRole("button", { name: "Run queued", exact: true }),
  ).toBeDisabled();
  expect(new Set(calls.lookups)).toEqual(new Set([accepted]));
  expect(calls.creates).toEqual([accepted]);
  await setRun(accepted, { status: "running", stage: "Fetching reports" });
  await expect(runStatus(page)).toContainText("Fetching reports…");
  await setRun(accepted, {
    status: "failed",
    completedAt: Timestamp.now(),
    error: "Tracked request stopped",
  });
  await expect(runButton(page)).toBeEnabled();
  await previousResults(page);
  await page.getByText("Run history", { exact: true }).click();
  await expect(
    page.getByText("Tracked request stopped", { exact: true }),
  ).toBeVisible();
  await admin(async (ctx) => {
    expect(
      (await getDoc(doc(ctx.firestore(), `${season}/runs/${accepted}`))).data()
        ?.fixtureMarker,
    ).toBe("accepted before response loss");
  });
});

test("recovers old queued work after reload and in another officer session", async ({
  page,
  browser,
}) => {
  expectedRequests = 1;
  await setRun("recovered", {
    status: "queued",
    requestedBy: "another-officer",
    requestedAt: Timestamp.fromMillis(Date.now() - 1800000),
  });
  await histories();
  await officer(page, emulator.environment);
  await expect(
    page.getByRole("button", { name: "Run queued", exact: true }),
  ).toBeDisabled();
  await expect(runStatus(page)).toContainText(
    "Queued · waiting for your local bot",
  );
  await previousResults(page);
  await page.reload();
  await expect(
    page.getByRole("button", { name: "Run queued", exact: true }),
  ).toBeDisabled();
  await previousResults(page);

  const context = await browser.newContext({
    viewport: { width: 390, height: 844 },
  });
  const other = await context.newPage();
  try {
    await officer(other, emulator.environment);
    await expect(
      other.getByRole("button", { name: "Run queued", exact: true }),
    ).toBeDisabled();
    await previousResults(other);
    for (const stage of ["Fetching reports", "Calculating results"]) {
      await setRun("recovered", { status: "running", stage });
      for (const view of [page, other]) {
        await expect(runStatus(view)).toContainText(`${stage}…`);
        await expect(
          view.getByRole("button", { name: "Running Pebble", exact: true }),
        ).toBeDisabled();
        await expect(
          view.getByText("Example Player", { exact: true }),
        ).toBeVisible();
      }
    }
    await page.reload();
    await expect(runStatus(page)).toContainText("Calculating results…");
    await previousResults(page);
    await setRun("recovered", {
      status: "failed",
      completedAt: Timestamp.now(),
      error: "Recovered request finished",
    });
    for (const view of [page, other]) {
      await expect(runButton(view)).toBeEnabled();
      await expect(
        view.getByText("Example Player", { exact: true }),
      ).toBeVisible();
      await view.getByText("Run history", { exact: true }).click();
      await expect(
        view.getByText("Recovered request finished", { exact: true }),
      ).toBeVisible();
    }
    expect(
      await other.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    ).toBe(true);
    expect((await requests()).map((r) => r.id)).toEqual(["recovered"]);
  } finally {
    await context.close();
  }
});
