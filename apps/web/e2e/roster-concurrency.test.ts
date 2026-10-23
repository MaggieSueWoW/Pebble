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
  query,
  where,
  runTransaction,
  serverTimestamp,
  setDoc,
  Timestamp,
} from "firebase/firestore";
import { readFileSync } from "node:fs";
import { officer } from "./officer";
import {
  readLinkedPlayers,
  advanceLinkedPlayers,
} from "../src/data/firestore/linkedPlayers";

const season = "seasons/test-reports";
let env: RulesTestEnvironment;
const main = (name: string) => ({
  name,
  className: "Mage",
  role: "Unknown",
  active: true,
  joinDate: "",
  leaveDate: "",
  notes: "Keep main fields",
  version: 1,
  updatedBy: "fixture",
  updatedAt: Timestamp.now(),
});
const alt = (playerId: string) => ({
  name: "Linkedalt",
  realm: "",
  region: "us",
  playerId,
  version: 1,
  updatedBy: "fixture",
  updatedAt: Timestamp.now(),
});
const availability = {
  nightId: "2026-10-15",
  playerId: "alpha",
  pre: { mode: "automatic" },
  post: { mode: "available" },
  notes: "Original",
  version: 1,
  updatedBy: "fixture",
  updatedAt: Timestamp.now(),
};
const actions = [
  "create alt",
  "edit alt",
  "delete alt",
  "move alt in",
  "move alt out",
  "create availability",
  "edit availability",
] as const;
type Action = (typeof actions)[number];

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
test.afterAll(async () => env?.cleanup());

async function seed(action: Action) {
  await env.clearFirestore();
  await env.withSecurityRulesDisabled(async (ctx) => {
    const db = ctx.firestore();
    await setDoc(doc(db, "officers/peer"), { enabled: true });
    await setDoc(doc(db, `${season}/players/alpha`), main("Alpha"));
    await setDoc(doc(db, `${season}/players/beta`), main("Beta"));
    await setDoc(doc(db, `${season}/characters/unrelated`), {
      ...alt("beta"),
      name: "Unrelated",
    });
    if (action.includes("alt") && action !== "create alt")
      await setDoc(
        doc(db, `${season}/characters/linked`),
        alt(action === "move alt in" ? "beta" : "alpha"),
      );
    if (action === "edit availability")
      await setDoc(
        doc(db, `${season}/availabilityOverrides/2026-10-15_alpha`),
        availability,
      );
  });
}

async function concurrentChange(
  action: Action,
  afterReads?: () => Promise<void>,
) {
  const db = env.authenticatedContext("peer").firestore();
  const path = action.includes("alt")
    ? "characters/linked"
    : "availabilityOverrides/2026-10-15_alpha";
  await runTransaction(db, async (transaction) => {
    const ref = doc(db, `${season}/${path}`);
    const current = await transaction.get(ref);
    const playerIds = action.startsWith("move") ? ["alpha", "beta"] : ["alpha"];
    const players = await readLinkedPlayers(
      transaction,
      db,
      "test-reports",
      playerIds,
    );
    await afterReads?.();
    advanceLinkedPlayers(transaction, players, "peer");
    if (action === "delete alt") transaction.delete(ref);
    else
      transaction.set(ref, {
        ...(current.exists()
          ? current.data()
          : action.includes("alt")
            ? alt("alpha")
            : availability),
        ...(action === "move alt in"
          ? { playerId: "alpha" }
          : action === "move alt out"
            ? { playerId: "beta" }
            : {}),
        ...(action === "edit alt"
          ? { realm: "Changed Realm" }
          : action === "edit availability"
            ? { notes: "Changed availability" }
            : {}),
        version: current.exists() ? current.data().version + 1 : 1,
        updatedBy: "peer",
        updatedAt: serverTimestamp(),
      });
  });
}

async function confirmDeletion(page: Page) {
  await page.getByRole("button", { name: "Edit Alpha", exact: true }).click();
  await page.getByRole("button", { name: "Delete main", exact: true }).click();
  await page
    .getByRole("button", { name: "Confirm delete main", exact: true })
    .click();
}

for (const action of actions) {
  for (const point of [
    "after enumeration",
    "after transaction reads",
  ] as const) {
    test(`main deletion stops for concurrent ${action} ${point}, then retries the current cascade`, async ({
      page,
    }) => {
      await seed(action);
      await officer(page, env);
      await page
        .getByRole("navigation")
        .getByRole("button", { name: "Roster", exact: true })
        .click();
      await expect(
        page.getByRole("button", { name: "Edit Alpha", exact: true }),
      ).toBeVisible();
      let release!: () => void;
      let intercepted!: () => void;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      const reached = new Promise<void>((resolve) => {
        intercepted = resolve;
      });
      let held = false;
      // batchGet begins only after both dependency queries; commit begins after
      // all deletion reads. Test both gaps without sleeps or test-only app hooks.
      const endpoint =
        point === "after enumeration"
          ? /\/documents:batchGet(?:\?|$)/
          : /\/documents:commit(?:\?|$)/;
      await page.route(endpoint, async (route) => {
        if (
          !held &&
          (route.request().postData() ?? "").includes(`${season}/players/alpha`)
        ) {
          held = true;
          intercepted();
          await gate;
        }
        await route.continue();
      });
      await confirmDeletion(page);
      await reached;
      try {
        await concurrentChange(action);
      } finally {
        release();
      }
      await expect(page.getByRole("alert")).toContainText(
        "This main or its linked data changed",
      );
      await page.unroute(endpoint);
      await env.withSecurityRulesDisabled(async (ctx) => {
        const db = ctx.firestore();
        expect(
          (await getDoc(doc(db, `${season}/players/alpha`))).data(),
        ).toMatchObject({
          name: "Alpha",
          notes: "Keep main fields",
          version: 2,
        });
        const linked = await getDoc(
          doc(
            db,
            `${season}/${action.includes("alt") ? "characters/linked" : "availabilityOverrides/2026-10-15_alpha"}`,
          ),
        );
        expect(linked.exists()).toBe(action !== "delete alt");
        if (action === "edit alt")
          expect(linked.data()?.realm).toBe("Changed Realm");
        if (action === "edit availability")
          expect(linked.data()?.notes).toBe("Changed availability");
        if (action.startsWith("move"))
          expect(linked.data()?.playerId).toBe(
            action === "move alt in" ? "alpha" : "beta",
          );
      });
      // Reload the current versions and explicitly reconfirm the new cascade.
      await page.reload();
      await expect(
        page.getByRole("heading", { name: "Roster", exact: true }),
      ).toBeVisible();
      await confirmDeletion(page);
      await expect(
        page.getByRole("button", { name: "Edit Alpha", exact: true }),
      ).toHaveCount(0);
      await env.withSecurityRulesDisabled(async (ctx) => {
        const db = ctx.firestore();
        expect(
          (await getDoc(doc(db, `${season}/players/alpha`))).exists(),
        ).toBe(false);
        expect(
          (await getDoc(doc(db, `${season}/players/beta`))).data(),
        ).toMatchObject({
          name: "Beta",
          notes: "Keep main fields",
          version: action.startsWith("move") ? 2 : 1,
        });
        expect(
          (await getDoc(doc(db, `${season}/characters/unrelated`))).exists(),
        ).toBe(true);
        for (const family of ["characters", "availabilityOverrides"])
          expect(
            (
              await getDocs(
                query(
                  collection(db, `${season}/${family}`),
                  where("playerId", "==", "alpha"),
                ),
              )
            ).empty,
          ).toBe(true);
        if (action === "move alt out")
          expect(
            (await getDoc(doc(db, `${season}/characters/linked`))).data()
              ?.playerId,
          ).toBe("beta");
        expect((await getDocs(collection(db, `${season}/runs`))).empty).toBe(
          true,
        );
      });
    });
  }
}

for (const action of [
  "create alt",
  "move alt in",
  "create availability",
  "edit availability",
] as const) {
  test(`deletion wins against an in-flight ${action} without resurrection or orphan links`, async ({
    page,
  }) => {
    await seed(action);
    await officer(page, env);
    await page
      .getByRole("navigation")
      .getByRole("button", { name: "Roster", exact: true })
      .click();
    await expect(
      page.getByRole("button", { name: "Edit Alpha", exact: true }),
    ).toBeVisible();
    let release!: () => void;
    let intercepted!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const reached = new Promise<void>((resolve) => {
      intercepted = resolve;
    });
    const write = concurrentChange(action, async () => {
      intercepted();
      await gate;
    }).then(
      () => null,
      (error) => error,
    );
    await reached;
    try {
      await confirmDeletion(page);
      await expect(
        page.getByRole("button", { name: "Edit Alpha", exact: true }),
      ).toHaveCount(0);
    } finally {
      release();
    }
    expect(await write).toBeTruthy();
    await env.withSecurityRulesDisabled(async (ctx) => {
      const db = ctx.firestore();
      expect((await getDoc(doc(db, `${season}/players/alpha`))).exists()).toBe(
        false,
      );
      expect(
        (await getDoc(doc(db, `${season}/players/beta`))).data()?.version,
      ).toBe(1);
      for (const family of ["characters", "availabilityOverrides"])
        expect(
          (
            await getDocs(
              query(
                collection(db, `${season}/${family}`),
                where("playerId", "==", "alpha"),
              ),
            )
          ).empty,
        ).toBe(true);
      if (action === "move alt in")
        expect(
          (await getDoc(doc(db, `${season}/characters/linked`))).data()
            ?.playerId,
        ).toBe("beta");
      expect((await getDocs(collection(db, `${season}/runs`))).empty).toBe(
        true,
      );
    });
  });
}
