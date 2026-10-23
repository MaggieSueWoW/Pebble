import type { Mode } from "../../shared/model";
import type { RecordedAvailability } from "./resultProvenance";
const modes: Record<Mode, string> = {
  automatic: "Automatic",
  available: "Available",
  unavailable: "Unavailable",
  partial: "Partial availability",
  unavailableFor: "Timed unavailability",
};
const sources: Record<RecordedAvailability["source"], string> = {
  override: "Officer adjustment",
  last_fight: "Last non-Mythic boss participation",
  blocks: "Recorded participation",
  none: "No inferred participation",
};
export function AvailabilityExplanation({
  availability,
}: {
  availability?: RecordedAvailability;
}) {
  if (!availability)
    return <p>Availability source not recorded for this result.</p>;
  return (
    <section
      className="availability-explanation"
      aria-label="Recorded availability explanation"
    >
      <h3>Recorded availability</h3>
      <dl className="metadata-grid">
        {(
          [
            ["pre", "Before break"],
            ["post", "After break"],
          ] as const
        ).map(([half, label]) => (
          <div key={half}>
            <dt>{label}</dt>
            <dd>
              {availability[half].available
                ? "Recorded available"
                : "Recorded unavailable"}{" "}
              · Input: {modes[availability[half].mode]}
            </dd>
          </div>
        ))}
        <div>
          <dt>Overall source</dt>
          <dd>{sources[availability.source]}</dd>
        </div>
      </dl>
      <p>
        Recorded availability can cover part of the half. Played and bench
        minutes show the recorded totals. The overall source describes the row,
        not each half.
      </p>
    </section>
  );
}
