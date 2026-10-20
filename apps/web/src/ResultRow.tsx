import { useId } from "react";
import { AttendanceDetails } from "./AttendanceDetails";
import { ArrowRight, ChevronDown, ChevronRight, Settings2 } from "lucide-react";
import {
  minutes,
  type Player,
  type PlayerResult,
  type WeeklyPlayerResult,
} from "./model";

export function ResultRow({
  player,
  row,
  index,
  expanded,
  onExpand,
  onEdit,
  showAttendance = false,
}: {
  player: Player;
  row: PlayerResult | WeeklyPlayerResult;
  index: number;
  expanded: boolean;
  onExpand: () => void;
  onEdit?: () => void;
  showAttendance?: boolean;
}) {
  const total = row.played + row.bench;
  const detailId = useId();
  return (
    <>
      <tr>
        <td className="muted rank">{String(index + 1).padStart(2, "0")}</td>
        <td>
          <button
            className="result-name"
            onClick={onExpand}
            aria-expanded={expanded}
            aria-controls={detailId}
          >
            <span className="class-dot" style={{ background: player.color }} />
            <span>
              <strong style={{ color: player.color }}>{player.name}</strong>
              <span className="cell-subtitle">{player.className}</span>
            </span>
          </button>
        </td>
        <td className="numeric">{minutes(row.played)}</td>
        <td className="numeric bench-value">{minutes(row.bench)}</td>
        <td>
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
              ? `${Math.round((row.played / total) * 100)}% played`
              : "No recorded time"}
            {"overridden" in row && row.overridden ? " · adjusted" : ""}
          </span>
        </td>
        <td>
          <button
            className="icon-button"
            aria-label={`Details for ${player.name}`}
            onClick={onExpand}
            aria-expanded={expanded}
            aria-controls={detailId}
          >
            {expanded ? <ChevronDown size={17} /> : <ChevronRight size={17} />}
          </button>
        </td>
      </tr>
      <tr className="result-detail" hidden={!expanded}>
        <td colSpan={6}>
          <div
            id={detailId}
            role="region"
            aria-label={`Availability breakdown for ${player.name}`}
          >
            {"overridden" in row && (
              <>
                <span>
                  Before break played{" "}
                  <strong>
                    {row.prePlayed == null
                      ? "Not recorded"
                      : `${row.prePlayed} min`}
                  </strong>
                </span>
                <span>
                  After break played{" "}
                  <strong>
                    {row.postPlayed == null
                      ? "Not recorded"
                      : `${row.postPlayed} min`}
                  </strong>
                </span>
              </>
            )}
            <span>
              Before break bench <strong>{row.preBench} min</strong>
            </span>
            <span>
              After break bench <strong>{row.postBench} min</strong>
            </span>
            {"overridden" in row && (
              <span>
                Availability{" "}
                <strong>
                  {row.overridden
                    ? "Officer adjustment applied"
                    : "Automatically inferred"}
                </strong>
              </span>
            )}
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
