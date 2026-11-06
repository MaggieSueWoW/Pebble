import { useEffect, useState } from "react";
import { doc, getDocFromServer } from "firebase/firestore";
import { PanelHeader } from "../../components/PanelHeader";
import { SortButton } from "../../components/SortButton";
import { db, seasonId } from "../../data/firebase";
import type { Results } from "../../shared/model";
import { resultMinutes } from "../results/format";
import { AttendanceModels } from "./AttendanceModels";
import { percent } from "./attendanceProbability";

type NightStatuses = Record<string, Record<string, string>>;
type AttendanceRead =
  | { state: "loading" | "missing" | "error" }
  | { state: "ready"; byPlayer: NightStatuses };

const statusWords: Record<string, string> = {
  P: "Played",
  B: "Benched",
  O: "Out",
};

const columns = [
  ["name", "Player"],
  ["rate", "Attendance %"],
  ["played", "Played"],
  ["bench", "Bench"],
  ["possible", "Total possible"],
] as const;

function measureSummaryColumns(table: HTMLTableElement | null) {
  const header = table?.tHead?.rows[0];
  if (!table || !header) return;

  const cells = Array.from(header.cells).slice(0, columns.length);
  const updateOffsets = () => {
    let left = 0;
    for (let index = 1; index < cells.length; index += 1) {
      left += cells[index - 1].getBoundingClientRect().width;
      table.style.setProperty(
        `--attendance-column-${index + 1}-left`,
        `${left}px`,
      );
    }
  };
  updateOffsets();
  const observer = new ResizeObserver(updateOffsets);
  cells.forEach((cell) => observer.observe(cell, { box: "border-box" }));
  return () => observer.disconnect();
}

export function AttendancePage({ results }: { results: Results }) {
  const [read, setRead] = useState<AttendanceRead>({ state: "loading" });
  const [attempt, setAttempt] = useState(0);
  const [sort, setSort] = useState<{
    key: (typeof columns)[number][0];
    direction: "ascending" | "descending";
  }>({ key: "name", direction: "ascending" });
  const run = results.signature;
  useEffect(() => {
    if (!run) return;
    let current = true;
    void getDocFromServer(
      doc(db!, "seasons", seasonId, "results", run, "officer", "attendance"),
    )
      .then((snapshot) => {
        if (!current) return;
        setRead(
          snapshot.exists()
            ? { state: "ready", byPlayer: snapshot.data().byPlayer }
            : { state: "missing" },
        );
      })
      .catch(() => {
        if (current) setRead({ state: "error" });
      });
    return () => {
      current = false;
    };
  }, [run, attempt]);

  const players = results.players
    .flatMap(({ id, name, color, attendance }) =>
      attendance ? [{ id, name: name || id, color, attendance }] : [],
    )
    .sort((a, b) => {
      const byName =
        a.name.localeCompare(b.name, "en", { sensitivity: "base" }) ||
        a.id.localeCompare(b.id);
      const direction = sort.direction === "ascending" ? 1 : -1;
      if (sort.key === "name") return direction * byName;
      const left = a.attendance[sort.key];
      const right = b.attendance[sort.key];
      if (left == null || right == null) {
        return Number(left == null) - Number(right == null) || byName;
      }
      return direction * (left - right) || byName;
    });
  const nights =
    read.state === "ready"
      ? results.nights.map(({ night }) => night).sort()
      : [];
  const rates = players.flatMap(({ attendance: { rate } }) =>
    rate != null && Number.isFinite(rate)
      ? // The sheet models the one-decimal percentages written by the attendance export.
        [Math.min(100, Number((rate * 100).toFixed(1))) / 100]
      : [],
  );
  return (
    <div className="attendance-page">
      <section className="panel" aria-labelledby="season-attendance-title">
        <div className="panel-body attendance-panel-heading">
          <PanelHeader
            id="season-attendance-title"
            title="Season attendance"
            actions={
              nights.length > 0 && (
                <ul
                  className="attendance-legend"
                  aria-label="Nightly attendance legend"
                >
                  {Object.entries(statusWords).map(([status, label]) => (
                    <li key={status}>
                      <span className="attendance-status" data-status={status}>
                        {status}
                      </span>
                      <span>= {label}</span>
                    </li>
                  ))}
                  <li>Blank = No applicable status</li>
                </ul>
              )
            }
          />
        </div>
        {!run ? (
          <p className="empty-state">
            Run Pebble to publish season attendance.
          </p>
        ) : (
          <>
            {read.state === "loading" && (
              <p className="attendance-note muted" role="status">
                Loading nightly attendance…
              </p>
            )}
            {read.state === "missing" && (
              <p className="attendance-note muted">
                Run Pebble again to publish nightly attendance for this season.
              </p>
            )}
            {read.state === "error" && (
              <div className="attendance-note">
                <span role="alert">Couldn’t load nightly attendance.</span>{" "}
                <button
                  className="text-button"
                  onClick={() => {
                    setRead({ state: "loading" });
                    setAttempt((previous) => previous + 1);
                  }}
                >
                  Try again
                </button>
              </div>
            )}
            {players.length ? (
              <div
                className="table-scroll attendance-grid-scroll"
                role="region"
                aria-label="Attendance grid"
                tabIndex={0}
              >
                <table
                  ref={measureSummaryColumns}
                  className="data-table attendance-grid"
                  aria-label="Season attendance by player"
                >
                  <thead>
                    <tr>
                      {columns.map(([key, label]) => {
                        const direction =
                          sort.key === key ? sort.direction : undefined;
                        return (
                          <th key={key} scope="col" aria-sort={direction}>
                            <SortButton
                              label={label}
                              direction={direction}
                              onClick={() =>
                                setSort((current) => ({
                                  key,
                                  direction:
                                    current.key === key &&
                                    current.direction === "ascending"
                                      ? "descending"
                                      : "ascending",
                                }))
                              }
                            />
                          </th>
                        );
                      })}
                      {nights.map((night) => (
                        <th scope="col" key={night}>
                          <time dateTime={night}>{night}</time>
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {players.map(({ id, name, color, attendance }) => (
                      <tr key={id}>
                        <th scope="row">
                          <span className="row-name">
                            <strong style={{ color }}>{name}</strong>
                          </span>
                        </th>
                        <td>
                          {attendance.rate == null
                            ? "—"
                            : percent(attendance.rate)}
                        </td>
                        <td>{resultMinutes(attendance.played)}</td>
                        <td>{resultMinutes(attendance.bench)}</td>
                        <td>{resultMinutes(attendance.possible)}</td>
                        {nights.map((night) => {
                          const status =
                            read.state === "ready"
                              ? read.byPlayer[id]?.[night]
                              : undefined;
                          const label = status
                            ? [...status]
                                .map((letter) => statusWords[letter])
                                .join(", ")
                            : "No applicable status";
                          return (
                            <td
                              key={night}
                              className="attendance-status"
                              data-status={status}
                              title={label}
                            >
                              <span aria-hidden="true">{status}</span>
                              <span className="sr-only">{label}</span>
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p className="empty-state">
                No eligible players in the latest results.
              </p>
            )}
          </>
        )}
      </section>
      <AttendanceModels rates={rates} />
    </div>
  );
}
