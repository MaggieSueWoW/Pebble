import {
  ArrowUpRight,
  Check,
  CircleAlert,
  ExternalLink,
  FileText,
  Info,
  Plus,
  ShieldCheck,
} from "lucide-react";
import { demoCode, formatDate, type Report } from "./model";
import type { ReportEditor } from "./useReportEditor";
import type { ReactNode } from "react";
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
        <div className="section-title">
          <span className="icon-tile">
            <Plus size={20} />
          </span>
          <div>
            <h2>Add a report</h2>
            <p>Paste Warcraft Logs links or report codes, one per line.</p>
          </div>
          <Badge tone="purple">WARCRAFT LOGS</Badge>
        </div>
        <label className="sr-only" htmlFor="report-input">
          Warcraft Logs reports
        </label>
        <textarea
          id="report-input"
          rows={3}
          value={reportText}
          onChange={(e) => setReportText(e.target.value)}
          placeholder={
            "https://www.warcraftlogs.com/reports/…\nOr a 16-character report code"
          }
          spellCheck={false}
          aria-describedby="report-help"
          aria-invalid={parsed.some((p) => p.state === "invalid")}
        />
        {parsed.length > 0 && (
          <ul className="parse-results" aria-live="polite">
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
            <ShieldCheck size={14} /> Adding a report won’t start the bot.
          </span>
          <div className="button-row">
            {demo && (
              <button
                className="text-button"
                onClick={() => {
                  setReportText(
                    `${reportText.trim() ? reportText.trim() + "\n" : ""}https://www.warcraftlogs.com/reports/${demoCode}`,
                  );
                }}
              >
                Try a sample link <ArrowUpRight size={13} />
              </button>
            )}
            <button
              className="button primary compact"
              onClick={addReports}
              disabled={
                blocked ||
                !ready.length ||
                parsed.some((p) => p.state === "invalid")
              }
            >
              <Plus size={15} /> Add{" "}
              {ready.length > 1 ? `${ready.length} reports` : "report"}
            </button>
          </div>
        </div>
      </section>
      <section className="card reports-card" aria-busy={saving}>
        <div className="table-heading">
          <div>
            <h2>
              Your reports <span className="count-pill">{reports.length}</span>
            </h2>
            <p>Include the logs you want Pebble to use.</p>
          </div>
          <span className="small muted">All dates in Pacific time</span>
        </div>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Report</th>
                <th>Raid night</th>
                <th>Status</th>
                <th className="align-right">Include</th>
              </tr>
            </thead>
            <tbody>
              {reports.map((r) => {
                const enabled = includeEdits[r.code] ?? r.enabled;
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
                            {r.fetched ? r.title : r.code}
                            <ExternalLink size={12} />
                          </a>
                          <span className="report-code">
                            {r.fetched
                              ? r.code
                              : "Metadata appears after the next run"}
                          </span>
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
          to the next run.
        </div>
      </section>
    </>
  );
}
