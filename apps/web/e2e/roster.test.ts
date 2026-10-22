import { test, expect, type Page } from "@playwright/test";
import {
  initializeTestEnvironment,
  type RulesTestEnvironment,
} from "@firebase/rules-unit-testing";
import {
  collection,
  doc,
  getDoc,
  getDocs,
  setDoc,
  Timestamp,
} from "firebase/firestore";
import { readFileSync } from "node:fs";
import { officer } from "./officer";
const projectId = "demo-pebble-reports",
  season = "seasons/test-reports";
let env: RulesTestEnvironment;
const admin = (
  fn: Parameters<RulesTestEnvironment["withSecurityRulesDisabled"]>[0],
) => env.withSecurityRulesDisabled(fn);
const player = (name = "Example", version = 1) => ({
  name,
  className: "Druid",
  role: "Healer",
  active: true,
  joinDate: "",
  leaveDate: "",
  notes: "Private baseline",
  version,
  updatedBy: "fixture",
  updatedAt: Timestamp.now(),
});
async function open(page: Page) {
  await officer(page, env);
  await page
    .getByRole("navigation")
    .getByRole("button", { name: "Roster", exact: true })
    .click();
  await expect(
    page.getByRole("region", { name: "Roster players" }),
  ).toBeVisible();
}
const save = (page: Page) =>
  page.getByRole("button", { name: "Save main", exact: true });
test.beforeAll(async () => {
  env = await initializeTestEnvironment({
    projectId,
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
  await env.clearFirestore();
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), "bot/status"), {
      lastHeartbeatAt: Timestamp.now(),
    });
    await setDoc(doc(ctx.firestore(), `${season}/reports/AAAAAAAAAAAAAAAA`), {
      enabled: true,
      version: 1,
      notes: "Report baseline",
      updatedBy: "fixture",
      updatedAt: Timestamp.now(),
    });
  });
});
test.afterAll(async () => {
  await env?.cleanup();
});
test.afterEach(async () => {
  await admin(async (ctx) => {
    expect(
      (await getDocs(collection(ctx.firestore(), `${season}/runs`))).empty,
    ).toBe(true);
    expect(
      (
        await getDoc(doc(ctx.firestore(), `${season}/reports/AAAAAAAAAAAAAAAA`))
      ).data()?.version,
    ).toBe(1);
  });
});
test("adds a Unicode main, edits and archives the same ID, preserves notes and fits narrow screens", async ({
  page,
}) => {
  await open(page);
  await expect(
    page.getByText(
      "No mains in the roster. Add the first main to get started.",
    ),
  ).toBeVisible();
  await page.getByRole("button", { name: "Add main" }).click();
  await page.getByLabel("Main name", { exact: true }).fill("Étoile");
  await page.getByLabel("Class", { exact: true }).selectOption("Mage");
  await page.getByLabel("Join date", { exact: true }).fill("2026-10-01");
  await page.getByLabel("Leave date", { exact: true }).fill("2026-09-30");
  await expect(page.getByRole("alert")).toContainText(
    "Join date must be on or before",
  );
  await expect(save(page)).toBeDisabled();
  await page.getByLabel("Leave date", { exact: true }).fill("");
  await page.getByLabel("Private roster notes").fill("Private note");
  await expect(
    page.getByRole("button", { name: "Run Pebble", exact: true }),
  ).toBeDisabled();
  await save(page).click();
  await expect(
    page.getByRole("status", { name: "Roster changes" }),
  ).toContainText("Roster saved");
  let id = "";
  await admin(async (ctx) => {
    const docs = await getDocs(
      collection(ctx.firestore(), `${season}/players`),
    );
    expect(docs.size).toBe(1);
    id = docs.docs[0].id;
    expect(docs.docs[0].data()).toMatchObject({
      name: "Étoile",
      className: "Mage",
      role: "Unknown",
      version: 1,
      notes: "Private note",
    });
  });
  await page.getByLabel("Main name", { exact: true }).fill("Renamed");
  await page.getByRole("checkbox", { name: "Active", exact: true }).uncheck();
  await page.getByLabel("Leave date", { exact: true }).fill("2026-10-20");
  await save(page).click();
  await expect(save(page)).toBeDisabled();
  await expect(
    page.getByRole("button", { name: "Edit Renamed", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Edit Renamed", exact: true }),
  ).toContainText("Inactive");
  await admin(async (ctx) => {
    const docs = await getDocs(
      collection(ctx.firestore(), `${season}/players`),
    );
    expect(docs.size).toBe(1);
    expect(docs.docs[0].id).toBe(id);
    expect(docs.docs[0].data()).toMatchObject({
      name: "Renamed",
      active: false,
      leaveDate: "2026-10-20",
      version: 2,
      notes: "Private note",
    });
  });
  await page.setViewportSize({ width: 390, height: 844 });
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});
test("validates duplicates, protects drafts and reports an expected-version conflict without overwriting", async ({
  page,
}) => {
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), `${season}/players/alpha`), player());
    await setDoc(
      doc(ctx.firestore(), `${season}/players/beta`),
      player("Beta"),
    );
  });
  await open(page);
  await page.getByRole("button", { name: "Edit Example", exact: true }).click();
  await page.getByLabel("Main name", { exact: true }).fill("beta");
  await expect(page.getByRole("alert")).toContainText("already used");
  await page.getByLabel("Main name", { exact: true }).fill("Draft");
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /^Reports/ })
    .click();
  await expect(
    page.getByRole("dialog", { name: "Keep your changes?" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Keep editing", exact: true }).click();
  await admin(async (ctx) => {
    await setDoc(
      doc(ctx.firestore(), `${season}/players/alpha`),
      player("Remote", 2),
    );
  });
  await save(page).click();
  await expect(page.getByRole("alert")).toContainText(
    "Another officer changed this player",
  );
  await expect(page.getByLabel("Main name", { exact: true })).toHaveValue(
    "Draft",
  );
  await page
    .getByRole("button", { name: "Cancel roster edits", exact: true })
    .click();
  await expect(page.getByLabel("Main name", { exact: true })).toHaveValue(
    "Remote",
  );
  await expect(page.getByLabel("Private roster notes")).toHaveValue(
    "Private baseline",
  );
  await page.getByLabel("Main name", { exact: true }).fill("Accepted");
  await save(page).click();
  await expect(save(page)).toBeDisabled();
  await admin(async (ctx) => {
    expect(
      (await getDoc(doc(ctx.firestore(), `${season}/players/alpha`))).data(),
    ).toMatchObject({
      name: "Accepted",
      version: 3,
      notes: "Private baseline",
    });
    expect(
      (await getDoc(doc(ctx.firestore(), `${season}/players/beta`))).data()
        ?.version,
    ).toBe(1);
  });
});

test("collapses main details and confirms deletion of only the selected main and its linked data", async ({
  page,
}) => {
  await admin(async (ctx) => {
    const db = ctx.firestore();
    await setDoc(doc(db, `${season}/players/alpha`), player());
    await setDoc(doc(db, `${season}/players/beta`), player("Beta"));
    await setDoc(doc(db, `${season}/characters/alt`), {
      name: "Alt",
      realm: "",
      region: "us",
      playerId: "alpha",
      version: 1,
      updatedBy: "fixture",
      updatedAt: Timestamp.now(),
    });
    await setDoc(doc(db, `${season}/availabilityOverrides/2026-10-15_alpha`), {
      nightId: "2026-10-15",
      playerId: "alpha",
      pre: { mode: "automatic" },
      post: { mode: "unavailable" },
      notes: "",
      version: 1,
      updatedBy: "fixture",
      updatedAt: Timestamp.now(),
    });
  });
  await open(page);
  const main = page.getByRole("button", { name: "Edit Example", exact: true });
  await main.click();
  await expect(page.getByLabel("Main name", { exact: true })).toBeVisible();
  await page
    .getByRole("button", { name: "Collapse Example", exact: true })
    .click();
  await expect(page.getByLabel("Main name", { exact: true })).toHaveCount(0);
  await main.click();
  await page.getByRole("button", { name: "Delete main", exact: true }).click();
  await expect(
    page.getByRole("group", { name: "Confirm main deletion" }),
  ).toContainText("1 alt mapping, and 1 availability override");
  await page.getByRole("button", { name: "Keep main", exact: true }).click();
  await expect(main).toBeVisible();
  await page.getByRole("button", { name: "Delete main", exact: true }).click();
  await page
    .getByRole("button", { name: "Confirm delete main", exact: true })
    .click();
  await expect(main).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Edit Beta", exact: true }),
  ).toBeVisible();
  await admin(async (ctx) => {
    for (const path of [
      "players/alpha",
      "characters/alt",
      "availabilityOverrides/2026-10-15_alpha",
    ])
      expect(
        (await getDoc(doc(ctx.firestore(), `${season}/${path}`))).exists(),
      ).toBe(false);
    expect(
      (await getDoc(doc(ctx.firestore(), `${season}/players/beta`))).exists(),
    ).toBe(true);
  });
});
