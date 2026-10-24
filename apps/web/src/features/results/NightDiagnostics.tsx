import type { NightResult } from "../../shared/model";
import { minutes, wallTime } from "../../shared/model";
import type { RecordedNightDiagnostics } from "./resultProvenance";

const sources = {
  override: "Overridden",
  configured: "Configured",
  detected: "Detected",
  none: "",
};
/** The caller must supply only officer metadata for the displayed completed run. */
export function NightDiagnostics({
  night,
  recorded,
  diagnostic,
}: {
  night: string;
  recorded?: NightResult;
  diagnostic?: RecordedNightDiagnostics;
}) {
  const timing = recorded?.timing;
  const source = (key: keyof RecordedNightDiagnostics["timing"]) =>
    diagnostic ? sources[diagnostic.timing[key]] : "Source not recorded";
  return (
    <dl
      className="raid-time-summary"
      aria-label={`Night details for ${night}`}
      title="Times shown in Pacific"
    >
      <div>
        <dt>Start</dt>
        <dd>
          {wallTime(timing?.start ?? "")}
          <span>{source("start")}</span>
        </dd>
      </div>
      <div>
        <dt>End</dt>
        <dd>
          {wallTime(timing?.end ?? "")}
          <span>{source("end")}</span>
        </dd>
      </div>
      <div>
        <dt>Break</dt>
        <dd>
          {timing?.breakStart && timing.breakEnd
            ? `${wallTime(timing.breakStart)} – ${wallTime(timing.breakEnd)}${diagnostic?.breakMinutes != null ? ` (${Math.round(diagnostic.breakMinutes)} min)` : ""}`
            : "No break recorded"}
          <span>{source("break")}</span>
        </dd>
      </div>
      <div>
        <dt>Raid time</dt>
        <dd>
          {recorded ? minutes(Math.round(recorded.duration)) : "Not recorded"}
          <span>Excluding break</span>
        </dd>
      </div>
      <div>
        <dt>Source reports</dt>
        <dd>
          {diagnostic?.reportCount ?? recorded?.reports ?? "Not recorded"}
        </dd>
      </div>
      <div>
        <dt>Mythic encounters</dt>
        <dd>{diagnostic?.mythicFights ?? "Not recorded"}</dd>
      </div>
    </dl>
  );
}
