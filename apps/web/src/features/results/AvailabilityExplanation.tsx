import type { Mode } from "../../shared/model";
import type { RecordedAvailability } from "./resultProvenance";

const reasons: Record<Exclude<Mode, "automatic">, string> = {
  available: "Available throughout",
  unavailable: "Unavailable throughout",
  partial: "Available for part of the period",
  unavailableFor: "Unavailable for part of the period",
};
/** Explain only the recorded half; the overall source cannot identify its cause. */
export function AvailabilityExplanation({
  availability,
  half,
}: {
  availability?: RecordedAvailability;
  half: "pre" | "post";
}) {
  const value = availability?.[half];
  if (!value) return <>Not recorded</>;
  return (
    <>
      {value.mode === "automatic"
        ? `${value.available ? "Available" : "Unavailable"} · Detected`
        : reasons[value.mode]}
    </>
  );
}
