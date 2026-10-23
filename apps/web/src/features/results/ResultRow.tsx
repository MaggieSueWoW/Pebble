import { AvailabilityExplanation } from "./AvailabilityExplanation";
import { useId } from "react";
import { AttendanceDetails } from "./AttendanceDetails";
import { ArrowRight, ChevronDown, ChevronRight, Settings2 } from "lucide-react";
import {
  minutes,
  type Player,
  type PlayerResult,
  type WeeklyPlayerResult,
} from "../../shared/model";

export function ResultRow({
  player,
  row,
  index,
  expanded,
  onExpand,
  onEdit,
  showAttendance = false,
  rank,
}: {
  player: Player;
  row: PlayerResult | WeeklyPlayerResult;
  index: number;
  expanded: boolean;
  onExpand: () => void;
  onEdit?: () => void;
  showAttendance?: boolean;
  rank?: number;
}) {
  const total = row.played + row.bench;
  const detailId = useId();
  return (
    <>
      <tr className="interactive-row result-player-row" onClick={onExpand}>
        <td className="muted rank">
          {String(rank ?? index + 1).padStart(2, "0")}
        </td>
        <td>
          <button
            className="row-name result-name"
            aria-expanded={expanded}
            aria-controls={detailId}
          >
            <strong style={{ color: player.color }}>{player.name}</strong>
          </button>
        </td>
        <td className="numeric bench-value">{minutes(row.bench)}</td>
        <td className="numeric">{minutes(row.played)}</td>
        <td>
          <div className="result-breakdown">
            <div
              className="time-bar"
              aria-label={`${row.played} minutes played, ${row.bench} minutes benched`}
            >
              <span
                style={{ width: `${total ? (row.played / total) * 100 : 0}%` }}
              />
              <i />
            </div>
            <span className="small muted">
              {total
                ? `${Math.round((row.played / total) * 100)}%`
                : "No recorded time"}
            </span>
          </div>
        </td>
        <td>
          <button
            className="icon-button"
            aria-label={`Details for ${player.name}`}
            aria-expanded={expanded}
            aria-controls={detailId}
          >
            {expanded ? <ChevronDown size={17} /> : <ChevronRight size={17} />}
          </button>
        </td>
      </tr>
      <tr className="detail-row result-detail" hidden={!expanded}>
        <td colSpan={6}>
          <div
            id={detailId}
            role="region"
            aria-label={`Availability breakdown for ${player.name}`}
          >
            <div className="player-time-details">
              <table
                className="summary-table break-summary"
                aria-label="Time by break"
              >
                <thead>
                  <tr>
                    <th scope="col">Period</th>
                    <th scope="col">Bench</th>
                    {"overridden" in row && <th scope="col">Played</th>}
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <th scope="row">Before break</th>
                    <td>{row.preBench === 0 ? "-" : `${row.preBench} min`}</td>
                    {"overridden" in row && (
                      <td>
                        {row.prePlayed == null
                          ? "Not recorded"
                          : row.prePlayed === 0
                            ? "-"
                            : `${row.prePlayed} min`}
                      </td>
                    )}
                  </tr>
                  <tr>
                    <th scope="row">After break</th>
                    <td>
                      {row.postBench === 0 ? "-" : `${row.postBench} min`}
                    </td>
                    {"overridden" in row && (
                      <td>
                        {row.postPlayed == null
                          ? "Not recorded"
                          : row.postPlayed === 0
                            ? "-"
                            : `${row.postPlayed} min`}
                      </td>
                    )}
                  </tr>
                </tbody>
              </table>
              {"overridden" in row && (
                <details className="detail-disclosure">
                  <summary>Availability explanation</summary>
                  <p>
                    {row.overridden
                      ? "Officer adjustment applied"
                      : "Automatically inferred"}
                  </p>
                  <AvailabilityExplanation availability={row.availability} />
                </details>
              )}
            </div>
            {showAttendance && (
              <AttendanceDetails
                attendance={"attendance" in row ? row.attendance : undefined}
              />
            )}
            {onEdit && (
              <button className="text-button" onClick={onEdit}>
                <Settings2 size={14} /> Edit availability{" "}
                <ArrowRight size={14} />
              </button>
            )}
          </div>
        </td>
      </tr>
    </>
  );
}
