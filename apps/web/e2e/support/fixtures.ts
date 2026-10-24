import type { RulesTestContext } from "@firebase/rules-unit-testing";
import { doc, setDoc, Timestamp } from "firebase/firestore";

export function metadata(version = 1) {
  return { version, updatedBy: "fixture", updatedAt: Timestamp.now() };
}

export async function seedHeartbeat(
  db: ReturnType<RulesTestContext["firestore"]>,
  lastHeartbeatAt = Timestamp.now(),
) {
  await setDoc(doc(db, "bot/status"), { lastHeartbeatAt });
}
