import { initializeApp } from "firebase/app";
import {
  connectAuthEmulator,
  getAuth,
  GoogleAuthProvider,
  signInWithPopup,
  signOut,
} from "firebase/auth";
import { connectFirestoreEmulator, getFirestore } from "firebase/firestore";

const env = import.meta.env;
export const configured = Boolean(
  env.VITE_FIREBASE_PROJECT_ID &&
  env.VITE_FIREBASE_API_KEY &&
  env.VITE_FIREBASE_APP_ID,
);
export const seasonId = env.VITE_PEBBLE_SEASON_ID || "midnight-s2";
export const seasonLabel = env.VITE_PEBBLE_SEASON_LABEL || "Midnight S2";
export const emulated = env.VITE_FIREBASE_USE_EMULATORS === "true";
const app = configured
  ? initializeApp({
      apiKey: env.VITE_FIREBASE_API_KEY,
      authDomain:
        env.VITE_FIREBASE_AUTH_DOMAIN ||
        `${env.VITE_FIREBASE_PROJECT_ID}.firebaseapp.com`,
      projectId: env.VITE_FIREBASE_PROJECT_ID,
      appId: env.VITE_FIREBASE_APP_ID,
    })
  : null;
export const auth = app ? getAuth(app) : null;
export const db = app ? getFirestore(app) : null;
if (emulated && auth && db) {
  connectAuthEmulator(
    auth,
    env.VITE_FIREBASE_AUTH_EMULATOR_URL || "http://127.0.0.1:9099",
    { disableWarnings: true },
  );
  connectFirestoreEmulator(
    db,
    env.VITE_FIRESTORE_EMULATOR_HOST || "127.0.0.1",
    Number(env.VITE_FIRESTORE_EMULATOR_PORT || 8080),
  );
}
export async function signIn() {
  if (!auth) throw new Error("Firebase is not configured.");
  const provider = new GoogleAuthProvider();
  provider.setCustomParameters({ prompt: "select_account" });
  await signInWithPopup(auth, provider);
}
export async function logOut() {
  if (auth) await signOut(auth);
}
