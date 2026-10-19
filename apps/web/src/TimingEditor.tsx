import { Check, CircleAlert, Clock3, RotateCcw } from "lucide-react";
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
    <section className="card timing-card" aria-label="Raid timing editor">
      <div role="status" aria-label="Timing changes" aria-live="polite">
        {saving ? "Saving timing…" : message}
      </div>
      {error && (
        <p role="alert" className="field-error">
          {error}
        </p>
      )}
      <div className="section-title">
        <span className="icon-tile">
          <Clock3 size={20} />
        </span>
        <div>
          <h2>A little timing correction</h2>
          <p>
            Leave a field empty to use the detected time. All times are Pacific.
          </p>
        </div>
      </div>
      {!timingFields.some(({ key }) => detected[key]) && (
        <p className="muted small">
          No detected timing for this night yet. Leave fields empty for
          automatic timing on the next run, or enter Pacific times.
        </p>
      )}
      <div className="timeline">
        <span>
          Raid start
          <strong>{wallTime(detected.start)}</strong>
        </span>
        <div className="timeline-track">
          <i />
          <b />
          <i />
        </div>
        <span>
          Raid end
          <strong>{wallTime(detected.end)}</strong>
        </span>
      </div>
      <div className="timing-fields">
        {timingFields.map((item) => (
          <div className="field" key={item.key}>
            <label>
              {item.label}
              <input
                type="datetime-local"
                aria-invalid={Boolean(timingError)}
                aria-describedby={`timing-${item.key}-help${timingError ? " timing-validation" : ""}`}
                value={timingEdit[item.key]}
                onInput={(e) => {
                  const value = e.currentTarget.value;
                  setTimingEdit((old) => ({
                    ...old,
                    [item.key]: value,
                  }));
                }}
              />
            </label>
            <span id={`timing-${item.key}-help`} className="muted small">
              Detected: {formatDate(night)} · {wallTime(detected[item.key])}
            </span>
          </div>
        ))}
      </div>
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
            disabled={!timingDirty || blocked}
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
