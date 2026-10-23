import type { RecordedNightDiagnostics } from "./resultProvenance";
const sources = {
  override: "Officer timing override",
  configured: "Configured default",
  detected: "Detected from logs",
  none: "No break recorded",
};
/** The caller must supply only officer metadata for the displayed completed run. */
export function NightDiagnostics({
  night,
  diagnostic,
}: {
  night: string;
  diagnostic?: RecordedNightDiagnostics;
}) {
  return (
    <details
      className="card night-diagnostics"
      aria-label="Officer night diagnostics"
    >
      <summary>Officer night diagnostics · {night}</summary>
      {!diagnostic ? (
        <p>No recorded diagnostics for this night.</p>
      ) : (
        <dl className="metadata-grid">
          <div>
            <dt>Source reports</dt>
            <dd>{diagnostic.reportCount}</dd>
          </div>
          <div>
            <dt>Mythic encounters</dt>
            <dd>{diagnostic.mythicFights}</dd>
          </div>
          <div>
            <dt>Unmapped characters</dt>
            <dd>{diagnostic.unmappedCharacters}</dd>
          </div>
          <div>
            <dt>Recorded break</dt>
            <dd>
              {diagnostic.breakMinutes == null
                ? "No break recorded"
                : `${diagnostic.breakMinutes} min`}
            </dd>
          </div>
          <div>
            <dt>Raid start source</dt>
            <dd>{sources[diagnostic.timing.start]}</dd>
          </div>
          <div>
            <dt>Raid end source</dt>
            <dd>{sources[diagnostic.timing.end]}</dd>
          </div>
          <div>
            <dt>Break source</dt>
            <dd>{sources[diagnostic.timing.break]}</dd>
          </div>
        </dl>
      )}
    </details>
  );
}
