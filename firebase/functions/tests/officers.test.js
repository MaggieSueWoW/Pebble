import { before, beforeEach, after, test } from "node:test";
import assert from "node:assert/strict";
import { initializeApp, deleteApp } from "firebase-admin/app";
import { getFirestore, Timestamp } from "firebase-admin/firestore";
import { manageOfficers } from "../officers.js";

// Never run administrative integration tests against a hosted database.
if (!process.env.FIRESTORE_EMULATOR_HOST)
  throw new Error("Start the Firestore emulator before running these tests.");
const app = initializeApp({ projectId: "demo-pebble-reports" });
const db = getFirestore(app);
const auth = (uid, email = `${uid}@example.test`) => ({
  uid,
  token: {
    email,
    name: `Officer ${uid}`,
    email_verified: true,
    firebase: { sign_in_provider: "google.com" },
  },
});
const call = (uid, data, email) =>
  manageOfficers(db, { auth: auth(uid, email), data });
const rejects = (promise, code) =>
  assert.rejects(promise, (e) => e.code === code);
const seed = (uid) =>
  db
    .doc(`officers/${uid}`)
    .set({
      enabled: true,
      email: `${uid}@example.test`,
      name: `Officer ${uid}`,
    });
const invite = async (email = "recipient@example.test") =>
  (await call("first", { action: "invite", email })).invitationId;
before(() => db.settings({ ignoreUndefinedProperties: false }));
beforeEach(async () => {
  for (const name of [
    "officers",
    "officerInvitations",
    "officerAudit",
    "officerManagement",
  ])
    await db.recursiveDelete(db.collection(name));
  await seed("first");
});
after(async () => {
  await db.terminate();
  await deleteApp(app);
});

test("only authenticated, verified Google identities and current officers administer access", async () => {
  await rejects(
    manageOfficers(db, { data: { action: "invite", email: "x@example.test" } }),
    "unauthenticated",
  );
  for (const token of [
    { email_verified: false },
    { firebase: { sign_in_provider: "password" } },
  ]) {
    const actor = auth("first");
    Object.assign(actor.token, token);
    await rejects(
      manageOfficers(db, {
        auth: actor,
        data: { action: "invite", email: "x@example.test" },
      }),
      "permission-denied",
    );
  }
  for (const action of ["invite", "cancel", "remove"])
    await rejects(
      call("outsider", {
        action,
        email: "x@example.test",
        uid: "first",
        invitationId: "a".repeat(48),
      }),
      "permission-denied",
    );
});

test("invitation lifecycle binds verified email to UID, deduplicates, records attribution, and supports normal sign-in", async () => {
  const id = await invite("  Recipient@Example.test ");
  assert.equal(await invite(), id);
  const own = await call("recipient", { action: "session" });
  assert.equal(own.invitations[0].id, id);
  const wrong = await call("other", { action: "session", invitationId: id });
  assert.equal(wrong.invitation.matchesAccount, false);
  await rejects(
    call("other", { action: "accept", invitationId: id }),
    "permission-denied",
  );
  await call("recipient", { action: "accept", invitationId: id });
  await call("recipient", { action: "accept", invitationId: id });
  const membership = (await db.doc("officers/recipient").get()).data();
  assert.equal(membership.enabled, true);
  assert.equal(membership.grantedBy, "first");
  assert.equal((await db.collection("officerAudit").get()).size, 2);
  await call("recipient", { action: "invite", email: "next@example.test" });
  await rejects(invite(), "already-exists");
});

test("expired, cancelled, malformed and nonexistent invitations cannot grant access; renewals use new links", async () => {
  const expired = await invite();
  await db
    .doc(`officerInvitations/${expired}`)
    .update({ expiresAt: Timestamp.fromMillis(1) });
  await rejects(
    call("recipient", { action: "accept", invitationId: expired }),
    "failed-precondition",
  );
  assert.equal(
    (await call("recipient", { action: "session", invitationId: expired }))
      .invitation.status,
    "expired",
  );
  const renewed = await invite();
  assert.notEqual(renewed, expired);
  await call("first", { action: "cancel", invitationId: renewed });
  await rejects(
    call("recipient", { action: "accept", invitationId: renewed }),
    "failed-precondition",
  );
  await rejects(
    call("recipient", { action: "accept", invitationId: "../first" }),
    "invalid-argument",
  );
  await rejects(
    call("recipient", { action: "accept", invitationId: "0".repeat(48) }),
    "not-found",
  );
  assert.equal((await db.doc("officers/recipient").get()).exists, false);
});

test("concurrent officer removals always leave one officer", async () => {
  await seed("second");
  const results = await Promise.allSettled([
    call("first", { action: "remove", uid: "second" }),
    call("second", { action: "remove", uid: "first" }),
  ]);
  assert.equal(results.filter((r) => r.status === "fulfilled").length, 1);
  assert.equal(
    (await db.collection("officers").where("enabled", "==", true).get()).size,
    1,
  );
  const remaining = (
    await db.collection("officers").where("enabled", "==", true).get()
  ).docs[0].id;
  await rejects(
    call(remaining, { action: "remove", uid: remaining }),
    "failed-precondition",
  );
});

test("removal cancels outstanding invitations without revoking existing descendants or allowing replay to regrant", async () => {
  const id = await invite();
  await call("recipient", { action: "accept", invitationId: id });
  const pending = await invite("pending@example.test");
  await call("recipient", { action: "remove", uid: "first" });
  await rejects(
    call("pending", { action: "accept", invitationId: pending }),
    "failed-precondition",
  );
  await rejects(
    call("first", { action: "invite", email: "blocked@example.test" }),
    "permission-denied",
  );
  assert.equal((await db.doc("officers/recipient").get()).data().enabled, true);
  await seed("second");
  await call("second", { action: "remove", uid: "recipient" });
  await call("recipient", { action: "accept", invitationId: id });
  assert.equal(
    (await db.doc("officers/recipient").get()).data().enabled,
    false,
  );
});

test("concurrent duplicate invites produce one link and acceptance versus cancellation is atomic", async () => {
  const ids = await Promise.all([invite(), invite()]);
  assert.equal(ids[0], ids[1]);
  await Promise.allSettled([
    call("recipient", { action: "accept", invitationId: ids[0] }),
    call("first", { action: "cancel", invitationId: ids[0] }),
  ]);
  const accepted =
    (await db.doc(`officerInvitations/${ids[0]}`).get()).data().status ===
    "accepted";
  assert.equal(
    (await db.doc("officers/recipient").get()).data()?.enabled === true,
    accepted,
  );
});

test("session refreshes identity metadata but cannot enable a revoked account", async () => {
  await db.doc("officers/first").set({ enabled: true });
  await call("first", { action: "session" });
  assert.equal(
    (await db.doc("officers/first").get()).data().email,
    "first@example.test",
  );
  await db.doc("officers/first").update({ enabled: false });
  await call("first", { action: "session" });
  assert.equal((await db.doc("officers/first").get()).data().enabled, false);
});
