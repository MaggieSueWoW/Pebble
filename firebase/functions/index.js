import { initializeApp } from "firebase-admin/app";
import { getFirestore } from "firebase-admin/firestore";
import { onCall } from "firebase-functions/v2/https";
import { manageOfficers } from "./officers.js";

initializeApp();

// Rare administrative operations can tolerate cold starts. No warm instances.
export const officerAccess = onCall(
  {
    region: "us-central1",
    minInstances: 0,
    maxInstances: 2,
    memory: "256MiB",
    cpu: "gcf_gen1",
    concurrency: 1,
    timeoutSeconds: 30,
  },
  (request) => manageOfficers(getFirestore(), request),
);
