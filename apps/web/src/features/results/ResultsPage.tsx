import { CheckCheck, FileText, Link2 } from "lucide-react";
import { Notice } from "../../components/Feedback";
import { AttendanceDetails } from "./AttendanceDetails";
import { DataTable } from "../../components/DataTable";
import { formatDate, type Player, type Results } from "../../shared/model";
import { ResultWarnings } from "../review/ResultWarnings";
import { warningsForNight } from "../review/warnings";
import { ResultRow } from "./ResultRow";
import { RunStatus } from "../../components/RunStatus";
import { SelectControl } from "../../components/SelectControl";
import { SortButton } from "../../components/SortButton";
import type { LiveConnection } from "../../data/useFirestore";
import type { ResultsView } from "./useResultsView";
export function ResultsPage({
  results,
  players,
  view,
  hasResults,
  canManage,
  officer,
  demo,
  sources,
  runStatusMessage,
  completedAt,
  onReviewNight,
  onEditAvailability,
}: {
  results: Results;
  players: Player[];
  view: ResultsView;
  hasResults: boolean;
  canManage: boolean;
  officer: boolean;
  demo: boolean;
  sources: LiveConnection["resultSources"];
  onReviewNight?: () => void;
  runStatusMessage: string;
  completedAt?: string;
  onEditAvailability: (id: string) => void;
}) {
  const {
    resultRows,
    additionalAttendance,
    visibleResultNight,
    resultNight,
    expandedPlayer,
    resultLinkNotice,
    setResultLinkNotice,
    setResultNight,
    setExpandedPlayer,
    resultSelectionReady,
    copyResultLink,
    activeResultSort,
    changeResultSort,
  } = view;
  return (
    <>
      {resultLinkNotice &&
        resultLinkNotice.period === resultNight &&
        resultLinkNotice.player === expandedPlayer && (
          <p role="status">{resultLinkNotice.text}</p>
        )}
      {hasResults && (
        <>
          {officer && onReviewNight ? (
            Boolean(
              warningsForNight(
                results.warnings,
                visibleResultNight === "season"
                  ? undefined
                  : visibleResultNight,
              ).length,
            ) && (
              <Notice tone="amber">
                This completed run has warnings.{" "}
                <button className="text-button" onClick={onReviewNight}>
                  Review night checks →
                </button>
              </Notice>
            )
          ) : (
            <ResultWarnings
              warnings={warningsForNight(results.warnings)}
              sources={officer ? sources : []}
            />
          )}
          <section className="card results-card">
            <div className="results-toolbar toolbar">
              <div className="result-period-select">
                <SelectControl
                  aria-label="Result period"
                  value={visibleResultNight}
                  onChange={(e) => {
                    setResultLinkNotice(null);
                    setResultNight(e.target.value);
                  }}
                >
                  <option value="season">Season to date</option>
                  {results.nights.map((n) => (
                    <option value={n.night} key={n.night}>
                      {formatDate(n.night, true)}
                    </option>
                  ))}
                </SelectControl>
              </div>
              <span className="result-night-count">
                {results.nights.length} raid{" "}
                {results.nights.length === 1 ? "night" : "nights"} this season
              </span>
              <RunStatus message={runStatusMessage} completedAt={completedAt} />
              <button
                className="text-button"
                onClick={() => void copyResultLink()}
                disabled={!resultSelectionReady}
              >
                <Link2 size={14} /> Copy result link
              </button>
            </div>
            <DataTable className="results-table">
              <thead>
                <tr>
                  <th>#</th>
                  {(
                    [
                      ["name", "Player"],
                      ["bench", "Bench"],
                      ["played", "Played"],
                      ["share", "Time Played"],
                    ] as const
                  ).map(([key, label]) => (
                    <th
                      key={key}
                      scope="col"
                      aria-sort={
                        activeResultSort.key === key
                          ? activeResultSort.direction
                          : undefined
                      }
                    >
                      <SortButton
                        label={label}
                        direction={
                          activeResultSort.key === key
                            ? activeResultSort.direction
                            : undefined
                        }
                        onClick={() => changeResultSort(key)}
                      />
                    </th>
                  ))}
                  <th />
                </tr>
              </thead>
              <tbody>
                {resultRows.map((row, i) => {
                  const player = row.name
                    ? {
                        id: row.id,
                        name: row.name,
                        className: row.className ?? "",
                        role: row.role ?? "",
                        color: row.color ?? "#b9c7d8",
                        pre: 0,
                        post: 0,
                      }
                    : players.find((p) => p.id === row.id)!;
                  return (
                    <ResultRow
                      key={row.id}
                      player={player}
                      row={row}
                      showAttendance={visibleResultNight === "season"}
                      index={i}
                      rank={
                        visibleResultNight === "season"
                          ? row.ranking?.rank
                          : undefined
                      }
                      expanded={expandedPlayer === row.id}
                      onExpand={() =>
                        setExpandedPlayer(
                          expandedPlayer === row.id ? null : row.id,
                        )
                      }
                      onEdit={
                        !canManage
                          ? undefined
                          : () => onEditAvailability(row.id)
                      }
                    />
                  );
                })}
              </tbody>
            </DataTable>
            {!resultRows.length && (
              <div className="empty-state">
                <FileText size={28} />
                <h3>No player time for this period</h3>
                <p>
                  {canManage
                    ? "Use Reports in the sidebar to review the included logs."
                    : "No player time was recorded for this period in the completed results."}
                  {results.nights.length > 0 && " Try another result period."}
                </p>
              </div>
            )}
            <div className="table-footnote">
              <CheckCheck size={14} />{" "}
              {!hasResults
                ? "No completed result set loaded."
                : "Results update automatically after a successful run."}
            </div>
          </section>
          {additionalAttendance.length > 0 && (
            <section
              className="card additional-attendance"
              aria-labelledby="additional-attendance-title"
            >
              <h2 id="additional-attendance-title">Additional attendance</h2>
              <p>
                These published attendance records are separate from the bench
                ranking.
              </p>
              {additionalAttendance.map((row) => (
                <details key={row.id} open={expandedPlayer === row.id}>
                  <summary
                    onClick={(event) => {
                      event.preventDefault();
                      setExpandedPlayer(
                        expandedPlayer === row.id ? null : row.id,
                      );
                    }}
                  >
                    Attendance for {row.name ?? row.id}
                  </summary>
                  <AttendanceDetails attendance={row.attendance} />
                </details>
              ))}
            </section>
          )}
          {demo && (
            <Notice>
              These are illustrative sample calculations, not Pebble’s
              production fairness engine. Notes and officer details are kept out
              of the results view.
            </Notice>
          )}
        </>
      )}
    </>
  );
}
