import type { DocumentData } from "firebase/firestore";
import type { Versions } from "./types";
export const stableVersions = (v: Versions) =>
  JSON.stringify(Object.entries(v).sort(([a], [b]) => a.localeCompare(b)));
export const dataOnly = (d: DocumentData) =>
  Object.fromEntries(
    Object.entries(d).filter(
      ([k]) => !["version", "updatedBy", "updatedAt"].includes(k),
    ),
  );
export const compareActiveRuns = (a: DocumentData, b: DocumentData) =>
  Number(b.status === "running") - Number(a.status === "running") ||
  timestampISO(a.requestedAt).localeCompare(timestampISO(b.requestedAt));
export const timestampISO = (value: unknown) =>
  value && typeof (value as { toDate?: unknown }).toDate === "function"
    ? (value as { toDate: () => Date }).toDate().toISOString()
    : "";
