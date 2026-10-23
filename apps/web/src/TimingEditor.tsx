import { PanelHeader } from "./PanelHeader";
import { Check, CircleAlert, RotateCcw } from "lucide-react";
import { formatDate, wallTime, type Timing } from "./model";
import { timingFields } from "./timing";
import type { TimingEditorState } from "./useTimingEditor";
export function TimingEditor({
  night,
  detected,
  editor,
}: {
  night: string;
  detected: Timing;
  editor: TimingEditorState;
}) {
  const {
    value: timingEdit,
    change: setTimingEdit,
    dirty: timingDirty,
    validation: timingError,
    saving,
    blocked,
    error,
    message,
    save,
    cancel,
    reset,
  } = editor;
  return (
    <section
      className="card panel-body timing-card"
      aria-label="Raid timing editor"
    >
      <div
        className="form-status"
        role="status"
        aria-label="Timing changes"
        aria-live="polite"
      >
        {saving ? "Saving timing…" : message}
      </div>
      {error && (
        <p role="alert" className="field-error">
          {error}
        </p>
      )}
      <PanelHeader
        title="Timing overrides"
        actions={timingDirty && <span className="badge amber">Unsaved</span>}
      />
      <p className="timing-help">
        Pacific time. Leave overrides blank to use detected times.
      </p>
      {!timingFields.some(({ key }) => detected[key]) && (
        <p className="muted small">
          No detected timing for this night yet. Leave fields empty for
          automatic timing on the next run, or enter Pacific times.
        </p>
      )}
      <table
        className="summary-table timing-table"
        aria-label="Raid timing overrides"
      >
        <thead>
          <tr>
            <th scope="col">Event</th>
            <th scope="col">Detected</th>
            <th scope="col">Override</th>
          </tr>
        </thead>
        <tbody>
          {timingFields.map((item) => (
            <tr key={item.key}>
              <th scope="row">
                <label htmlFor={`timing-${item.key}`}>{item.label}</label>
              </th>
              <td id={`timing-${item.key}-help`} className="timing-detected">
                <span className="sr-only">Detected: </span>
                {detected[item.key] ? (
                  <>
                    {formatDate(detected[item.key].slice(0, 10) || night)} ·{" "}
                    {wallTime(detected[item.key])}
                  </>
                ) : (
                  "—"
                )}
              </td>
              <td className="field">
                <input
                  id={`timing-${item.key}`}
                  type="datetime-local"
                  aria-invalid={Boolean(timingError)}
                  aria-describedby={`timing-${item.key}-help${timingError ? " timing-validation" : ""}`}
                  value={timingEdit[item.key]}
                  onInput={(e) => {
                    const value = e.currentTarget.value;
                    setTimingEdit((old) => ({ ...old, [item.key]: value }));
                  }}
                />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <div className="field">
        <label>
          Notes <span className="optional">optional · private</span>
          <textarea
            rows={2}
            maxLength={500}
            placeholder="Anything useful about this night…"
            aria-label="Timing notes"
            value={timingEdit.notes}
            onChange={(e) =>
              setTimingEdit((old) => ({
                ...old,
                notes: e.target.value,
              }))
            }
          />
        </label>
      </div>
      {timingError && (
        <p id="timing-validation" role="alert" className="field-error">
          <CircleAlert size={14} />
          {timingError}
        </p>
      )}
      <div className="card-action-row">
        <button className="text-button" disabled={blocked} onClick={reset}>
          <RotateCcw size={14} /> Reset to automatic
        </button>
        <div className="button-row">
          <button
            className="button secondary"
            disabled={!timingDirty || saving}
            onClick={cancel}
          >
            Cancel
          </button>
          <button
            className="button primary"
            disabled={!timingDirty || Boolean(timingError) || blocked}
            onClick={save}
          >
            <Check size={15} /> Save timing
          </button>
        </div>
      </div>
    </section>
  );
}
