import { Badge } from "../../components/Feedback";
import { resultMinutes } from "./format";
import { AvailabilityExplanation } from "./AvailabilityExplanation";
import { useId } from "react";
import { AttendanceDetails } from "./AttendanceDetails";
import { ArrowRight, ChevronDown, ChevronRight, Settings2 } from "lucide-react";
import {
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
          <span className="result-player-name">
            <button
              className="row-name result-name"
              aria-expanded={expanded}
              aria-controls={detailId}
            >
              <strong style={{ color: player.color }}>{player.name}</strong>
            </button>
            {!showAttendance && "overridden" in row && row.overridden && (
              <Badge tone="purple">Adjusted</Badge>
            )}
          </span>
        </td>
        <td className="numeric bench-value">{resultMinutes(row.bench)}</td>
        <td className="numeric">{resultMinutes(row.played)}</td>
        <td>
          <div className="result-breakdown">
            <div
              className="time-bar"
              aria-label={`${Math.round(row.played)} minutes played, ${Math.round(row.bench)} minutes benched`}
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
                    {!showAttendance && "overridden" in row && (
                      <th scope="col">Reason</th>
                    )}
                  </tr>
                </thead>
                <tbody>
                  {(
                    [
                      ["pre", "Before break"],
                      ["post", "After break"],
                    ] as const
                  ).map(([half, label]) => {
                    const availability =
                      "availability" in row ? row.availability : undefined;
                    const adjusted =
                      !showAttendance &&
                      availability?.[half] != null &&
                      availability[half].mode !== "automatic";
                    const bench = half === "pre" ? row.preBench : row.postBench;
                    const played =
                      "overridden" in row
                        ? half === "pre"
                          ? row.prePlayed
                          : row.postPlayed
                        : undefined;
                    return (
                      <tr key={half}>
                        <th scope="row">{label}</th>
                        <td
                          className={
                            adjusted ? "result-adjusted-time" : undefined
                          }
                          title={adjusted ? "Availability adjusted" : undefined}
                        >
                          {resultMinutes(bench)}
                        </td>
                        {"overridden" in row && (
                          <td
                            className={
                              adjusted && played != null
                                ? "result-adjusted-time"
                                : undefined
                            }
                            title={
                              adjusted && played != null
                                ? "Availability adjusted"
                                : undefined
                            }
                          >
                            {played == null
                              ? "Not recorded"
                              : resultMinutes(played)}
                          </td>
                        )}
                        {!showAttendance && "overridden" in row && (
                          <td>
                            <AvailabilityExplanation
                              availability={availability}
                              half={half}
                            />
                          </td>
                        )}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
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
