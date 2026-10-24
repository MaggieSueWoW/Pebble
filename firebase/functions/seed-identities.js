import { initializeApp } from "firebase-admin/app";
import { getAuth } from "firebase-admin/auth";
import { getFirestore } from "firebase-admin/firestore";

// Run once with administrator ADC to name existing UID-only memberships.
const projectId = process.argv[2];
if (!projectId)
  throw new Error(
    "Usage: node firebase/functions/seed-identities.js FIREBASE_PROJECT_ID",
  );
initializeApp({ projectId });
const db = getFirestore();
const officers = await db
  .collection("officers")
  .where("enabled", "==", true)
  .get();
let updated = 0;
let unavailable = 0;
for (const doc of officers.docs) {
  try {
    const user = await getAuth().getUser(doc.id);
    if (!user.email) {
      unavailable += 1;
      continue;
    }
    // Update identity only; a concurrent revocation must remain revoked.
    await doc.ref.update({
      email: user.email.trim().toLowerCase(),
      name: user.displayName || user.email,
    });
    updated += 1;
  } catch (error) {
    if (error.code !== "auth/user-not-found") throw error;
    unavailable += 1;
  }
}
console.log(
  `Updated ${updated} officer identities; ${unavailable} accounts unavailable.`,
);
await db.terminate();
