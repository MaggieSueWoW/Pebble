import { formatTime, formatRunDate } from "../shared/model";

/** One timestamp format and live announcement region across all workflow pages. */
export function RunStatus({
  message,
  completedAt,
  detail,
}: {
  message: string;
  completedAt?: string;
  detail?: string;
}) {
  return (
    <div
      className="run-status"
      role="status"
      aria-label="Run status"
      aria-atomic="true"
    >
      {message && <strong>{message}</strong>}
      {completedAt ? (
        <span>
          Updated{" "}
          <time dateTime={completedAt}>
            {formatTime(completedAt)} PT · {formatRunDate(completedAt)}
          </time>
        </span>
      ) : (
        <span>No completed runs yet</span>
      )}
      {detail && <span>{detail}</span>}
    </div>
  );
}
