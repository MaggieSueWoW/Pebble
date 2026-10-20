import type { Mode } from "./model";
/** Half flags are recorded engine values; source is whole-row priority. */
export type RecordedAvailability = {
  pre: { available: boolean; mode: Mode };
  post: { available: boolean; mode: Mode };
  source: "override" | "last_fight" | "blocks" | "none";
};
/** Officer-only metadata, matched to the displayed completed run. */
export type RecordedNightDiagnostics = {
  reportCount: number;
  mythicFights: number;
  unmappedCharacters: number;
  breakMinutes: number | null;
  timing: {
    start: "override" | "configured" | "detected";
    end: "override" | "detected";
    break: "override" | "detected" | "none";
  };
};
