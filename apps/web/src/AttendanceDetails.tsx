import { useId } from "react";
import { formatDate, type AttendanceProjection } from "./model";
const recordedMinutes = (value: number) =>
  Number.isFinite(value) && value >= 0 ? `${value} min` : "Unavailable";
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
      <h3 id={title}>Attendance</h3>
      {!attendance ? (
        <p>
          Attendance unavailable. This result set does not include attendance
          for this player.
        </p>
      ) : (
        <>
          <dl className="attendance-totals">
            <div>
              <dt>Attendance rate</dt>
              <dd>{rate}</dd>
            </div>
            <div>
              <dt>Eligible played</dt>
              <dd>{recordedMinutes(attendance.played)}</dd>
            </div>
            <div>
              <dt>Eligible bench</dt>
              <dd>{recordedMinutes(attendance.bench)}</dd>
            </div>
            <div>
              <dt>Possible minutes</dt>
              <dd>{recordedMinutes(attendance.possible)}</dd>
            </div>
          </dl>
          <p>
            Attendance counts eligible played and bench time against possible
            time. It differs from the played share above. These minutes use
            eligible membership nights and may differ from the season totals.
          </p>
          {attendance.possible === 0 && (
            <p>
              No possible time was recorded; attendance rate is unavailable.
            </p>
          )}
          <p className="attendance-legend">
            P = played · B = benched · O = out. A week can contain several
            statuses; an empty status means no applicable event.
          </p>
          {weeks.length ? (
            <table className="attendance-weeks">
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
                    <td>
                      <span aria-label={statusWords(status)}>
                        {status || "No applicable event"}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <p>No weekly attendance was published.</p>
          )}
        </>
      )}
    </section>
  );
}
