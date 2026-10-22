import {
  ArrowUpRight,
  Check,
  CircleAlert,
  ExternalLink,
  FileText,
  Info,
} from "lucide-react";
import { demoCode, formatDate, type Report } from "./model";
import type { ReportEditor } from "./useReportEditor";
import { useState, type ReactNode } from "react";
import { SortButton } from "./SortButton";
function Badge({ children, tone }: { children: ReactNode; tone: string }) {
  return <span className={`badge ${tone}`}>{children}</span>;
}
export function ReportsPanel({
  reports,
  editor,
  demo,
}: {
  reports: Report[];
  editor: ReportEditor;
  demo: boolean;
}) {
  const [direction, setDirection] = useState<"ascending" | "descending">(
    "ascending",
  );
  const sortedReports = [...reports].sort((a, b) => {
    const left = a.fetched ? a.night : "";
    const right = b.fetched ? b.night : "";
    if (!left || !right)
      return left ? -1 : right ? 1 : a.code.localeCompare(b.code);
    return (
      (direction === "ascending" ? 1 : -1) * left.localeCompare(right) ||
      a.code.localeCompare(b.code)
    );
  });
  const {
    text: reportText,
    setText: setReportText,
    edits: includeEdits,
    parsed,
    ready,
    saving,
    blocked,
    error,
    message,
    add: addReports,
    reenable,
    toggle,
    cancelSelection,
    saveSelection: saveInclusion,
  } = editor;
  return (
    <>
      {error && (
        <div className="notice red" role="alert">
          {error}
        </div>
      )}
      <div role="status" aria-label="Report changes" aria-live="polite">
        {saving ? "Saving report changes…" : message}
      </div>
      <section className="card add-card" aria-busy={saving}>
        <form
          className="report-entry"
          onSubmit={(event) => {
            event.preventDefault();
            if (
              !blocked &&
              ready.length &&
              !parsed.some((item) => item.state === "invalid")
            )
              void addReports();
          }}
        >
          <label className="sr-only" htmlFor="report-input">
            Warcraft Logs reports
          </label>
          <input
            id="report-input"
            type="text"
            value={reportText}
            onChange={(e) => setReportText(e.target.value)}
            placeholder="Warcraft Logs link or report code"
            spellCheck={false}
            aria-describedby={
              parsed.some((p) => p.state === "invalid")
                ? "report-help report-validation"
                : "report-help"
            }
            aria-invalid={parsed.some((p) => p.state === "invalid")}
          />
          <button
            className="button primary compact"
            type="submit"
            disabled={
              blocked ||
              !ready.length ||
              parsed.some((p) => p.state === "invalid")
            }
          >
            Add {ready.length > 1 ? `${ready.length} reports` : "report"}
          </button>
        </form>
        {parsed.length > 0 && (
          <ul
            id="report-validation"
            className="parse-results"
            aria-live="polite"
          >
            {parsed.map((item, i) => (
              <li
                key={`${item.raw}-${i}`}
                className={item.state === "invalid" ? "invalid" : ""}
              >
                {item.state === "ready" ? (
                  <Check size={14} />
                ) : item.state === "invalid" ? (
                  <CircleAlert size={14} />
                ) : (
                  <Info size={14} />
                )}
                <code>{item.code || item.raw}</code>
                <span>{item.message}</span>
                {item.state === "disabled" && (
                  <button
                    className="text-button"
                    disabled={blocked}
                    onClick={() => reenable(item.code)}
                  >
                    Re-enable
                  </button>
                )}
              </li>
            ))}
          </ul>
        )}
        <div className="card-action-row">
          <span className="muted small" id="report-help">
            Paste a Warcraft Logs link or 16-character code. Adding a report
            won’t start the bot.
          </span>
          <div className="button-row">
            {demo && (
              <button
                className="text-button"
                onClick={() => {
                  setReportText(
                    `https://www.warcraftlogs.com/reports/${demoCode}`,
                  );
                }}
              >
                Try a sample link <ArrowUpRight size={13} />
              </button>
            )}
          </div>
        </div>
      </section>
      <section className="card reports-card" aria-busy={saving}>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Report</th>
                <th scope="col" aria-sort={direction}>
                  <SortButton
                    label="Raid night (PT)"
                    direction={direction}
                    onClick={() =>
                      setDirection((current) =>
                        current === "ascending" ? "descending" : "ascending",
                      )
                    }
                  />
                </th>
                <th>Status</th>
                <th className="align-right">Include</th>
              </tr>
            </thead>
            <tbody>
              {!reports.length && (
                <tr>
                  <td colSpan={4} className="empty-state">
                    No reports yet. Paste a Warcraft Logs link or report code in
                    the field above.
                  </td>
                </tr>
              )}
              {sortedReports.map((r) => {
                const enabled = includeEdits[r.code] ?? r.enabled;
                const checkedAt =
                  r.checkedAt && Number.isFinite(Date.parse(r.checkedAt))
                    ? r.checkedAt
                    : "";
                return (
                  <tr key={r.code} className={enabled ? "" : "excluded-row"}>
                    <td>
                      <div className="report-cell">
                        <span
                          className={`report-icon ${!r.fetched ? "unfetched" : ""}`}
                        >
                          <FileText size={18} />
                        </span>
                        <div>
                          <a
                            href={`https://www.warcraftlogs.com/reports/${r.code}`}
                            target="_blank"
                            rel="noreferrer"
                          >
                            {r.fetched && r.title.trim() ? r.title : r.code}
                            <ExternalLink size={12} />
                          </a>
                          <div className="report-metadata">
                            <span className="report-code">
                              {r.fetched
                                ? r.code
                                : "Metadata appears after the next run"}
                            </span>
                            <span className="cell-subtitle">
                              {checkedAt ? (
                                <>
                                  Last checked{" "}
                                  <time dateTime={checkedAt}>
                                    {new Intl.DateTimeFormat("en-US", {
                                      year: "numeric",
                                      month: "short",
                                      day: "numeric",
                                      hour: "numeric",
                                      minute: "2-digit",
                                      timeZone: "America/Los_Angeles",
                                      timeZoneName: "short",
                                    }).format(new Date(checkedAt))}
                                  </time>
                                </>
                              ) : (
                                "Last checked unavailable"
                              )}
                            </span>
                          </div>
                        </div>
                      </div>
                    </td>
                    <td>
                      {r.fetched ? (
                        <>
                          <span className="cell-title">
                            {formatDate(r.night)}
                          </span>
                          <span className="cell-subtitle">
                            {new Intl.DateTimeFormat("en-US", {
                              weekday: "long",
                              timeZone: "UTC",
                            }).format(new Date(`${r.night}T12:00:00Z`))}
                          </span>
                        </>
                      ) : (
                        <span className="muted">—</span>
                      )}
                    </td>
                    <td>
                      <Badge
                        tone={
                          !enabled ? "neutral" : !r.fetched ? "amber" : "green"
                        }
                      >
                        {!enabled
                          ? "Excluded"
                          : !r.fetched
                            ? "Not fetched"
                            : "Processed"}
                      </Badge>
                    </td>
                    <td className="align-right">
                      <button
                        className={`switch ${enabled ? "on" : ""}`}
                        role="switch"
                        aria-checked={enabled}
                        aria-label={`Include ${r.code}`}
                        disabled={blocked}
                        onClick={() => toggle(r.code)}
                      >
                        <span />
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        {Object.keys(includeEdits).length > 0 && (
          <div className="inclusion-save">
            <span className="small">
              {Object.keys(includeEdits).length} unsaved selection change
              {Object.keys(includeEdits).length === 1 ? "" : "s"}
            </span>
            <div className="button-row">
              <button
                className="button ghost compact"
                disabled={blocked}
                onClick={cancelSelection}
              >
                Cancel
              </button>
              <button
                className="button primary compact"
                disabled={blocked}
                onClick={saveInclusion}
              >
                Save selection
              </button>
            </div>
          </div>
        )}
        <div className="table-footnote">
          <Info size={14} /> Excluded reports stay here. They won’t contribute
          to the next run. Report details update when Pebble runs.
        </div>
      </section>
    </>
  );
}
