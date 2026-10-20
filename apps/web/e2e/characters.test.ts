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
const season = "seasons/test-reports";
let env: RulesTestEnvironment;
const admin = (
  fn: Parameters<RulesTestEnvironment["withSecurityRulesDisabled"]>[0],
) => env.withSecurityRulesDisabled(fn);
const meta = (version = 1) => ({
  version,
  updatedBy: "fixture",
  updatedAt: Timestamp.now(),
});
const alias = (version = 1, realm = "Example Realm") => ({
  name: "Étoile",
  realm,
  region: "us",
  playerId: "main",
  ...meta(version),
});
const save = (page: Page) =>
  page.getByRole("button", { name: "Save mapping", exact: true });
async function open(page: Page) {
  await officer(page, env);
  await page
    .getByRole("navigation")
    .getByRole("button", { name: "Roster", exact: true })
    .click();
  await expect(
    page.getByRole("heading", { name: "Character mappings", exact: true }),
  ).toBeVisible();
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
  await env.clearFirestore();
  await admin(async (ctx) => {
    const db = ctx.firestore();
    for (const [id, name, active] of [
      ["main", "Example", true],
      ["archived", "Archived", false],
    ] as const)
      await setDoc(doc(db, `${season}/players/${id}`), {
        name,
        active,
        className: "Druid",
        role: "Healer",
        joinDate: "",
        leaveDate: "",
        notes: "Private roster baseline",
        ...meta(),
      });
    await setDoc(doc(db, "bot/status"), { lastHeartbeatAt: Timestamp.now() });
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
    for (const id of ["main", "archived"])
      expect(
        (await getDoc(doc(ctx.firestore(), `${season}/players/${id}`))).data()
          ?.version,
      ).toBe(1);
  });
});
test("creates normalized mappings, rejects duplicate identity and allows a distinct region and wildcard realm", async ({
  page,
}) => {
  await open(page);
  await page.getByRole("button", { name: "Add mapping", exact: true }).click();
  await page.getByLabel("Character name", { exact: true }).fill(" Étoile ");
  await page.getByLabel("Realm", { exact: true }).fill(" Example Realm ");
  await expect(save(page)).toBeDisabled();
  await expect(page.getByRole("alert")).toContainText(
    "Choose an existing main",
  );
  await page.getByLabel("Mapped main", { exact: true }).selectOption("main");
  await expect(
    page.getByRole("button", { name: "Run Pebble", exact: true }),
  ).toHaveCount(0);
  await save(page).click();
  await expect(
    page.getByRole("status", { name: "Character mapping changes" }),
  ).toContainText("Character mapping saved");
  await expect(page.getByLabel("Character name", { exact: true })).toHaveValue(
    "Étoile",
  );
  await expect(page.getByLabel("Realm", { exact: true })).toHaveValue(
    "Example Realm",
  );
  await page.getByRole("button", { name: "Add mapping", exact: true }).click();
  await page.getByLabel("Character name", { exact: true }).fill("éTOILE");
  await page.getByLabel("Realm", { exact: true }).fill("EXAMPLE REALM");
  await page.getByLabel("Mapped main", { exact: true }).selectOption("main");
  await expect(page.getByRole("alert")).toContainText("already have a mapping");
  await expect(save(page)).toBeDisabled();
  await page.getByLabel("Region", { exact: true }).selectOption("eu");
  await save(page).click();
  await expect(
    page.getByRole("status", { name: "Character mapping changes" }),
  ).toContainText("Character mapping saved");
  await expect(save(page)).toBeDisabled();
  await page.getByRole("button", { name: "Add mapping", exact: true }).click();
  await page.getByLabel("Character name", { exact: true }).fill("Étoile");
  await page
    .getByLabel("Mapped main", { exact: true })
    .selectOption("archived");
  await expect(
    page.getByLabel("Mapped main", { exact: true }).locator("option:checked"),
  ).toHaveText("Archived (inactive)");
  await save(page).click();
  await expect(
    page.getByRole("status", { name: "Character mapping changes" }),
  ).toContainText("Character mapping saved");
  await expect(save(page)).toBeDisabled();
  await admin(async (ctx) => {
    const docs = await getDocs(
      collection(ctx.firestore(), `${season}/characters`),
    );
    expect(docs.size).toBe(3);
    expect(docs.docs.map((d) => d.data())).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "Étoile",
          realm: "Example Realm",
          region: "us",
          playerId: "main",
          version: 1,
        }),
        expect.objectContaining({ realm: "EXAMPLE REALM", region: "eu" }),
        expect.objectContaining({
          realm: "",
          region: "us",
          playerId: "archived",
        }),
      ]),
    );
  });
  await page.setViewportSize({ width: 390, height: 844 });
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});
test("retains mapping drafts on conflict, guards navigation, then reassigns the same stable ID", async ({
  page,
}) => {
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), `${season}/characters/stable`), alias());
  });
  await open(page);
  await page.getByLabel("Realm", { exact: true }).fill("Draft Realm");
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
      doc(ctx.firestore(), `${season}/characters/stable`),
      alias(2, "Remote Realm"),
    );
  });
  await save(page).click();
  await expect(page.getByRole("alert")).toContainText(
    "Another officer changed this mapping",
  );
  await expect(page.getByLabel("Realm", { exact: true })).toHaveValue(
    "Draft Realm",
  );
  await page
    .getByRole("button", { name: "Cancel mapping edits", exact: true })
    .click();
  await expect(page.getByLabel("Realm", { exact: true })).toHaveValue(
    "Remote Realm",
  );
  await page
    .getByLabel("Mapped main", { exact: true })
    .selectOption("archived");
  await page.getByLabel("Character name", { exact: true }).fill("Renamed");
  await save(page).click();
  await expect(
    page.getByRole("status", { name: "Character mapping changes" }),
  ).toContainText("Character mapping saved");
  await expect(save(page)).toBeDisabled();
  await expect(
    page.getByLabel("Character mapping", { exact: true }),
  ).toHaveValue("stable");
  await admin(async (ctx) => {
    const docs = await getDocs(
      collection(ctx.firestore(), `${season}/characters`),
    );
    expect(docs.size).toBe(1);
    expect(docs.docs[0].id).toBe("stable");
    expect(docs.docs[0].data()).toMatchObject({
      name: "Renamed",
      realm: "Remote Realm",
      region: "us",
      playerId: "archived",
      version: 3,
    });
  });
});
