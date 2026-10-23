import { Clock3, ChevronDown, Check, CircleAlert } from "lucide-react";
import type { History } from "../shared/model";
import { formatTime } from "../shared/model";
import { formatRunDate } from "../components/RunStatus";
export function RunHistory({
  history,
  connected,
}: {
  history: History[];
  connected: boolean;
}) {
  return (
    <details className="card run-history">
      <summary>
        <div>
          <Clock3 size={16} />
          <strong>Run history</strong>
          <span className="muted small">
            {history.length} completed {history.length === 1 ? "run" : "runs"}
          </span>
        </div>
        <ChevronDown size={16} />
      </summary>
      <div>
        {!history.length && (
          <p className="empty-state">No completed runs in recent history.</p>
        )}
        {history.map((h) => (
          <div className="history-row" key={h.id}>
            {h.status === "succeeded" ? (
              <Check size={16} className="green-text" />
            ) : (
              <CircleAlert size={16} className="red-text" />
            )}
            <div>
              <strong>
                {h.status === "succeeded"
                  ? connected
                    ? "Computation completed"
                    : "Results updated"
                  : "Run failed"}
              </strong>
              <p>{h.message}</p>
            </div>
            <span className="muted small">
              {formatRunDate(h.at)} · {formatTime(h.at)} PT
            </span>
          </div>
        ))}
      </div>
    </details>
  );
}
