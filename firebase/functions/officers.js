import { randomBytes } from "node:crypto";
import { Timestamp } from "firebase-admin/firestore";
import { HttpsError } from "firebase-functions/v2/https";

const fail = (code, message) => {
  throw new HttpsError(code, message);
};
const emailKey = (value) =>
  typeof value === "string" ? value.trim().toLowerCase() : "";
const validEmail = (value) =>
  value.length <= 254 && /^[^\s@/]+@[^\s@/]+\.[^\s@/]+$/.test(value);
const validId = (value) =>
  typeof value === "string" && /^[a-f0-9]{48}$/.test(value);
const pending = (data, now) =>
  data.status === "pending" && data.expiresAt.toMillis() > now;
const identity = (auth) => {
  if (!auth) fail("unauthenticated", "Sign in with Google to continue.");
  if (
    auth.token.firebase?.sign_in_provider !== "google.com" ||
    auth.token.email_verified !== true
  )
    fail("permission-denied", "Use a verified Google account.");
  const email = emailKey(auth.token.email);
  if (!validEmail(email))
    fail(
      "permission-denied",
      "Your Google account needs a verified email address.",
    );
  return {
    uid: auth.uid,
    email,
    name: String(auth.token.name || email).slice(0, 200),
  };
};
const publicInvite = (doc) => {
  const data = doc.data();
  return {
    id: doc.id,
    email: data.email,
    invitedByName: data.invitedByName,
    expiresAt: data.expiresAt.toDate().toISOString(),
    status: data.status,
  };
};

/** All authority changes share a transaction lock, including phantom/query races. */
export async function manageOfficers(db, request) {
  const actor = identity(request.auth);
  const input = request.data || {};
  const action = input.action;
  const actorRef = db.doc(`officers/${actor.uid}`);
  const invites = db.collection("officerInvitations");

  if (action === "session") {
    // Only write identity metadata when it changes. Never grant access here.
    await db.runTransaction(async (tx) => {
      const own = await tx.get(actorRef);
      if (
        own.data()?.enabled &&
        (own.data().email !== actor.email || own.data().name !== actor.name)
      )
        tx.update(actorRef, { email: actor.email, name: actor.name });
    });
    const matches = await invites.where("email", "==", actor.email).get();
    const now = Date.now();
    const invitations = matches.docs
      .filter((d) => pending(d.data(), now))
      .map(publicInvite);
    let invitation = null;
    if (input.invitationId) {
      if (!validId(input.invitationId))
        fail("invalid-argument", "This invitation link is invalid.");
      const linked = await invites.doc(input.invitationId).get();
      if (!linked.exists)
        fail("not-found", "This invitation no longer exists.");
      invitation = {
        ...publicInvite(linked),
        matchesAccount: linked.data().email === actor.email,
        status: pending(linked.data(), now)
          ? "pending"
          : linked.data().status === "pending"
            ? "expired"
            : linked.data().status,
      };
    }
    return { invitations, invitation };
  }
  if (!["invite", "accept", "cancel", "remove"].includes(action))
    fail("invalid-argument", "Unknown officer action.");
  const email = emailKey(input.email);
  if (action === "invite" && !validEmail(email))
    fail("invalid-argument", "Enter a Google account email address.");
  if (["accept", "cancel"].includes(action) && !validId(input.invitationId))
    fail("invalid-argument", "This invitation link is invalid.");
  if (
    action === "remove" &&
    (typeof input.uid !== "string" ||
      !input.uid ||
      input.uid.includes("/") ||
      input.uid.length > 128)
  )
    fail("invalid-argument", "Choose an officer to remove.");

  // Fixed across transaction retries; request replay is also safe for acceptance/removal.
  const newInvite = invites.doc(randomBytes(24).toString("hex"));
  const audit = db.collection("officerAudit").doc();
  return db.runTransaction(async (tx) => {
    const lock = db.doc("officerManagement/state");
    const [state, own] = await Promise.all([tx.get(lock), tx.get(actorRef)]);
    const now = Timestamp.now();
    const isOfficer = own.data()?.enabled === true;
    if (action !== "accept" && !isOfficer)
      fail("permission-denied", "Current officer access is required.");
    const log = (details) => {
      tx.set(lock, { version: (state.data()?.version || 0) + 1 });
      tx.create(audit, {
        action,
        actorUid: actor.uid,
        actorName: actor.name,
        at: now,
        ...details,
      });
    };
    if (action === "invite") {
      const [officers, existing] = await Promise.all([
        tx.get(db.collection("officers").where("enabled", "==", true)),
        tx.get(invites.where("status", "==", "pending")),
      ]);
      if (
        email === actor.email ||
        officers.docs.some((d) => emailKey(d.data().email) === email)
      )
        fail("already-exists", "This account already has officer access.");
      const active = existing.docs.filter((d) =>
        pending(d.data(), now.toMillis()),
      );
      const duplicate = active.find((d) => d.data().email === email);
      if (duplicate) return { invitationId: duplicate.id };
      if (active.length >= 100)
        fail(
          "resource-exhausted",
          "Cancel unused invitations before adding more.",
        );
      // Retire expired records for this email; old links never become valid again.
      existing.docs
        .filter((d) => d.data().email === email)
        .forEach((d) => tx.update(d.ref, { status: "expired" }));
      tx.create(newInvite, {
        email,
        status: "pending",
        invitedBy: actor.uid,
        invitedByName: actor.name,
        createdAt: now,
        expiresAt: Timestamp.fromMillis(
          now.toMillis() + 7 * 24 * 60 * 60 * 1000,
        ),
      });
      log({ invitationId: newInvite.id, email });
      return { invitationId: newInvite.id };
    }
    if (action === "accept" || action === "cancel") {
      const ref = invites.doc(input.invitationId);
      const invitation = await tx.get(ref);
      if (!invitation.exists)
        fail("not-found", "This invitation no longer exists.");
      const data = invitation.data();
      if (action === "accept") {
        if (data.email !== actor.email)
          fail(
            "permission-denied",
            `Sign in with ${data.email} to accept this invitation.`,
          );
        if (data.status === "accepted" && data.acceptedBy === actor.uid) {
          if (isOfficer) return { accepted: true };
          fail(
            "failed-precondition",
            "This invitation has already been used. Ask a current officer for a new invitation.",
          );
        }
        if (!pending(data, now.toMillis()))
          fail(
            "failed-precondition",
            "This invitation has expired or is no longer available.",
          );
        const inviter = await tx.get(db.doc(`officers/${data.invitedBy}`));
        if (!inviter.data()?.enabled)
          fail(
            "failed-precondition",
            "The inviting officer no longer has access. Ask a current officer for a new invitation.",
          );
        // A pending invite can outlive a separate administrator grant. Preserve
        // the original grant attribution when the account already has access.
        if (!isOfficer)
          tx.set(
            actorRef,
            {
              enabled: true,
              email: actor.email,
              name: actor.name,
              grantedBy: data.invitedBy,
              grantedByName: data.invitedByName,
              grantedAt: now,
            },
            { merge: true },
          );
        tx.update(ref, {
          status: "accepted",
          acceptedBy: actor.uid,
          acceptedAt: now,
        });
        log({ invitationId: ref.id, targetUid: actor.uid, email: actor.email });
        return { accepted: true };
      }
      if (data.status !== "pending")
        fail("failed-precondition", "This invitation is no longer pending.");
      tx.update(ref, {
        status: "cancelled",
        cancelledBy: actor.uid,
        cancelledAt: now,
      });
      log({ invitationId: ref.id, email: data.email });
      return { cancelled: true };
    }
    const targetRef = db.doc(`officers/${input.uid}`);
    const [target, officers, issued] = await Promise.all([
      tx.get(targetRef),
      tx.get(db.collection("officers").where("enabled", "==", true)),
      tx.get(invites.where("invitedBy", "==", input.uid)),
    ]);
    if (!target.data()?.enabled) return { removed: true };
    if (officers.size <= 1)
      fail(
        "failed-precondition",
        "The last officer cannot be removed. Add another officer first.",
      );
    tx.update(targetRef, {
      enabled: false,
      revokedBy: actor.uid,
      revokedAt: now,
    });
    issued.docs
      .filter((d) => pending(d.data(), now.toMillis()))
      .forEach((d) =>
        tx.update(d.ref, {
          status: "cancelled",
          cancelledBy: actor.uid,
          cancelledAt: now,
        }),
      );
    log({ targetUid: input.uid, email: target.data().email || null });
    return { removed: true };
  });
}
