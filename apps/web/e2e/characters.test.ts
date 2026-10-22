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
  page.getByRole("button", { name: "Save alt", exact: true });
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
async function addAltFor(page: Page, name: string) {
  const main = page.getByRole("button", { name: `Edit ${name}`, exact: true });
  if ((await main.getAttribute("aria-expanded")) !== "true") await main.click();
  await page
    .getByRole("region", { name: "Alts", exact: true })
    .getByRole("button", { name: "Add alt", exact: true })
    .click();
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
test.afterEach(async ({}, testInfo) => {
  await admin(async (ctx) => {
    expect(
      (await getDocs(collection(ctx.firestore(), `${season}/runs`))).empty,
    ).toBe(true);
    for (const id of ["main", "archived"])
      expect(
        (await getDoc(doc(ctx.firestore(), `${season}/players/${id}`))).data()
          ?.version,
      ).toBe(
        testInfo.title.startsWith("swaps an alt") && id === "main" ? 3 : 1,
      );
  });
});
test("creates normalized mappings, rejects duplicate identity and allows a distinct region and wildcard realm", async ({
  page,
}) => {
  await open(page);
  await addAltFor(page, "Example");
  await page.getByLabel("Character name", { exact: true }).fill(" Étoile ");
  await page.getByLabel("Realm", { exact: true }).fill(" Example Realm ");
  await expect(page.getByLabel("Mapped main", { exact: true })).toHaveCount(0);
  await expect(save(page)).toBeEnabled();
  await expect(
    page.getByRole("button", { name: "Run Pebble", exact: true }),
  ).toBeDisabled();
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
  await addAltFor(page, "Example");
  await page.getByLabel("Character name", { exact: true }).fill("éTOILE");
  await page.getByLabel("Realm", { exact: true }).fill("EXAMPLE REALM");
  await expect(page.getByRole("alert")).toContainText("already have a mapping");
  await expect(save(page)).toBeDisabled();
  await page.getByLabel("Region", { exact: true }).selectOption("eu");
  await save(page).click();
  await expect(
    page.getByRole("status", { name: "Character mapping changes" }),
  ).toContainText("Character mapping saved");
  await expect(save(page)).toBeDisabled();
  await addAltFor(page, "Archived");
  await page.getByLabel("Character name", { exact: true }).fill("Étoile");
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
test("retains mapping drafts on conflict, guards navigation, then edits the same stable ID", async ({
  page,
}) => {
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), `${season}/characters/stable`), alias());
  });
  await open(page);
  await page
    .getByRole("button", { name: "Edit alt Étoile for Example", exact: true })
    .click();
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
    .getByRole("button", { name: "Cancel alt edits", exact: true })
    .click();
  await expect(page.getByLabel("Realm", { exact: true })).toHaveValue(
    "Remote Realm",
  );
  await page.getByLabel("Character name", { exact: true }).fill("Renamed");
  await save(page).click();
  await expect(
    page.getByRole("status", { name: "Character mapping changes" }),
  ).toContainText("Character mapping saved");
  await expect(save(page)).toBeDisabled();
  await expect(
    page.getByRole("button", {
      name: "Edit alt Renamed for Example",
      exact: true,
    }),
  ).toBeVisible();
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
      playerId: "main",
      version: 3,
    });
  });
});

test("manages alts inside main details and deletes only the confirmed alt", async ({
  page,
}) => {
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), `${season}/characters/stable`), alias());
    await setDoc(doc(ctx.firestore(), `${season}/characters/other`), {
      ...alias(),
      name: "Secondalt",
    });
  });
  await open(page);
  await page.getByRole("button", { name: "Edit Example", exact: true }).click();
  await expect(page.getByLabel("Main name", { exact: true })).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Close main editor", exact: true }),
  ).toHaveCount(0);
  const alts = page.getByRole("region", { name: "Alts", exact: true });
  await expect(
    alts.getByRole("button", { name: "Étoile", exact: true }),
  ).toBeVisible();
  await alts
    .getByRole("button", { name: "Delete alt Étoile", exact: true })
    .click();
  await alts.getByRole("button", { name: "Keep alt", exact: true }).click();
  await expect(
    alts.getByRole("button", { name: "Étoile", exact: true }),
  ).toBeVisible();
  await alts
    .getByRole("button", { name: "Delete alt Étoile", exact: true })
    .click();
  await alts
    .getByRole("button", { name: "Confirm delete alt", exact: true })
    .click();
  await expect(
    alts.getByRole("button", { name: "Étoile", exact: true }),
  ).toHaveCount(0);
  await expect(
    alts.getByRole("button", { name: "Secondalt", exact: true }),
  ).toBeVisible();
  await admin(async (ctx) => {
    expect(
      (
        await getDoc(doc(ctx.firestore(), `${season}/characters/stable`))
      ).exists(),
    ).toBe(false);
    expect(
      (
        await getDoc(doc(ctx.firestore(), `${season}/characters/other`))
      ).exists(),
    ).toBe(true);
  });
});

test("swaps an alt into the main and back while retaining IDs, realm mapping, membership and other alts", async ({
  page,
}) => {
  await admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), `${season}/characters/stable`), alias());
    await setDoc(doc(ctx.firestore(), `${season}/characters/other`), {
      ...alias(),
      name: "Secondalt",
    });
  });
  await open(page);
  await page.getByRole("button", { name: "Edit Example", exact: true }).click();
  const alts = page.getByRole("region", { name: "Alts", exact: true });
  await alts
    .getByRole("button", { name: "Make Étoile main", exact: true })
    .click();
  await alts
    .getByRole("button", { name: "Swap main and alt", exact: true })
    .click();
  await expect(
    page.getByRole("button", { name: "Edit Étoile", exact: true }),
  ).toBeVisible();
  await expect(
    alts.getByRole("button", { name: "Example", exact: true }),
  ).toBeVisible();
  await expect(
    alts.getByRole("button", { name: "Étoile", exact: true }),
  ).toHaveCount(0);
  await admin(async (ctx) => {
    const db = ctx.firestore();
    expect(
      (await getDoc(doc(db, `${season}/players/main`))).data(),
    ).toMatchObject({
      name: "Étoile",
      active: true,
      className: "Druid",
      notes: "Private roster baseline",
      version: 2,
    });
    expect(
      (await getDoc(doc(db, `${season}/characters/stable`))).data(),
    ).toMatchObject({
      name: "Étoile",
      realm: "Example Realm",
      playerId: "main",
      version: 1,
    });
    const mappings = (
      await getDocs(collection(db, `${season}/characters`))
    ).docs.map((d) => d.data());
    expect(mappings).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "Example",
          realm: "",
          region: "us",
          playerId: "main",
        }),
        expect.objectContaining({ name: "Secondalt", playerId: "main" }),
      ]),
    );
  });
  await alts
    .getByRole("button", { name: "Make Example main", exact: true })
    .click();
  await alts
    .getByRole("button", { name: "Swap main and alt", exact: true })
    .click();
  await expect(
    page.getByRole("button", { name: "Edit Example", exact: true }),
  ).toBeVisible();
  await expect(
    alts.getByRole("button", { name: "Étoile", exact: true }),
  ).toBeVisible();
  await admin(async (ctx) => {
    expect(
      (await getDocs(collection(ctx.firestore(), `${season}/characters`))).size,
    ).toBe(3);
    expect(
      (await getDoc(doc(ctx.firestore(), `${season}/characters/stable`))).data()
        ?.realm,
    ).toBe("Example Realm");
  });
});
