import type { ReactNode } from "react";
import { MissingPlayers } from "./MissingPlayers";
import type { MissingPlayerEditor, Resolution } from "./useMissingPlayerEditor";
import type { MissingPlayer } from "../results/resultProvenance";
import type { LiveConnection } from "../../data/useFirestore";
import { Notice } from "../../components/Feedback";
import { SelectControl } from "../../components/SelectControl";
import { TimingEditor } from "../overrides/TimingEditor";
import type { TimingEditorState } from "../overrides/useTimingEditor";
import { NightDiagnostics } from "../results/NightDiagnostics";
import { ResultWarnings } from "./ResultWarnings";
import { warningsForNight } from "./warnings";
import { formatDate, type Results, type Timing } from "../../shared/model";

export function ReviewNightPage({
  night,
  nights,
  chooseNight,
  results,
  hasResults,
  sources,
  diagnostics,
  detected,
  editor,
  onRoster,
  connection,
  playerEditor,
  onPlayerAction,
  nextAction,
}: {
  night: string;
  nights: string[];
  chooseNight: (night: string) => void;
  results: Results;
  hasResults: boolean;
  sources: LiveConnection["resultSources"];
  diagnostics: LiveConnection["resultDiagnostics"];
  detected: Timing;
  editor: TimingEditorState;
  nextAction?: ReactNode;
  onRoster?: () => void;
  connection?: LiveConnection;
  playerEditor: MissingPlayerEditor;
  onPlayerAction: (player: MissingPlayer, mode: Resolution) => void;
}) {
  if (!nights.length)
    return (
      <Notice>
        Add a report and run Pebble to identify a raid night for review.
      </Notice>
    );
  const warnings = warningsForNight(results.warnings, night);
  const diagnostic = diagnostics[night];
  const recorded = results.nights.find((row) => row.night === night);
  return (
    <div className="night-review">
      <div className="toolbar">
        <label className="night-select">
          Raid night
          <SelectControl
            aria-label="Raid night"
            value={night}
            onChange={(event) => chooseNight(event.target.value)}
          >
            {nights.map((value) => (
              <option key={value} value={value}>
                {formatDate(value, true)}
              </option>
            ))}
          </SelectControl>
        </label>
      </div>
      <section className="card panel-body" aria-label="Raid night summary">
        <NightDiagnostics
          night={night}
          recorded={recorded}
          diagnostic={diagnostic}
        />
        <details className="timing-disclosure">
          <summary>
            Times look wrong? Adjust timing
            {editor.dirty && <span className="badge amber">Unsaved</span>}
          </summary>
          <TimingEditor night={night} detected={detected} editor={editor} />
        </details>
      </section>
      {nextAction}
      {hasResults ? (
        <>
          <ResultWarnings
            compact
            warnings={
              diagnostic?.missingPlayers?.length
                ? warnings.filter(
                    ({ text }) => !text.includes(": unknown characters:"),
                  )
                : warnings
            }
            sources={sources}
          />
          {connection?.officer && diagnostic?.missingPlayers ? (
            <MissingPlayers
              players={diagnostic.missingPlayers}
              connection={connection}
              editor={playerEditor}
              onAction={onPlayerAction}
            />
          ) : (
            <section className="card panel-body">
              <h2>Missing players</h2>
              {diagnostic?.unmappedCharacters ? (
                <p>
                  {diagnostic.unmappedCharacters} unmatched. Run Pebble to load
                  player actions.
                </p>
              ) : (
                <p>No warnings recorded for this night.</p>
              )}
              {onRoster && (
                <button className="text-button" onClick={onRoster}>
                  Update roster →
                </button>
              )}
            </section>
          )}
          {!recorded && <Notice>No calculated night totals.</Notice>}
        </>
      ) : (
        <Notice>Run Pebble to load night checks.</Notice>
      )}
    </div>
  );
}
