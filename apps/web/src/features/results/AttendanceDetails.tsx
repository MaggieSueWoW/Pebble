import { resultMinutes } from "./format";
import { useId } from "react";
import { formatDate, type AttendanceProjection } from "../../shared/model";
const recordedMinutes = (value: number) =>
  Number.isFinite(value) && value >= 0
    ? value === 0
      ? "0m"
      : resultMinutes(value)
    : "Unavailable";
const statusWords = (status: string) =>
  status
    ? Array.from(
        status,
        (letter) =>
          ({ P: "played", B: "benched", O: "out" })[
            letter as "P" | "B" | "O"
          ] ?? letter,
      ).join(", ")
    : "No applicable event";
/** Present only published attendance; eligibility and rates belong to the worker. */
export function AttendanceDetails({
  attendance,
}: {
  attendance?: AttendanceProjection;
}) {
  const title = useId();
  const rate =
    attendance &&
    attendance.possible > 0 &&
    attendance.rate != null &&
    Number.isFinite(attendance.rate)
      ? `${Number((attendance.rate * 100).toFixed(1))}%`
      : "Unavailable";
  const weeks = [...(attendance?.weeks ?? [])].sort((a, b) =>
    a.week.localeCompare(b.week),
  );
  return (
    <section className="attendance-details" aria-labelledby={title}>
      <h3 id={title} className="sr-only">
        Attendance
      </h3>
      <div className="attendance-summary">
        <strong>
          Attendance: <span>{rate}</span>
        </strong>
        {attendance && (
          <span>
            <span>({recordedMinutes(attendance.played)}</span> eligible played +{" "}
            <span>{recordedMinutes(attendance.bench)}</span> eligible bench) /{" "}
            <span>{recordedMinutes(attendance.possible)}</span> possible
          </span>
        )}
      </div>
      {!attendance ? (
        <p className="muted small">Attendance not recorded.</p>
      ) : (
        <>
          {attendance.possible === 0 && (
            <p className="muted small">No possible time recorded.</p>
          )}
          {weeks.length ? (
            <table className="summary-table attendance-weeks">
              <caption>Weekly attendance</caption>
              <thead>
                <tr>
                  <th scope="col">Week of</th>
                  <th scope="col">Status</th>
                </tr>
              </thead>
              <tbody>
                {weeks.map(({ week, status }) => (
                  <tr key={week}>
                    <th scope="row">{formatDate(week, true)}</th>
                    <td>{statusWords(status)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <p className="muted small">No weekly attendance recorded.</p>
          )}
        </>
      )}
    </section>
  );
}
