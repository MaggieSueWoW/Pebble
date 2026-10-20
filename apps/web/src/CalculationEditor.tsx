import type { CalculationEditorState } from "./useCalculationEditor";
export function CalculationEditor({
  editor,
}: {
  editor: CalculationEditorState;
}) {
  const {
    value,
    change,
    dirty,
    blocked,
    saving,
    validation,
    error,
    message,
    save,
    cancel,
  } = editor;
  const fields = [
    ["breakWindowStart", "Break window start", "time"],
    ["breakWindowEnd", "Break window end", "time"],
    ["minBreakMinutes", "Minimum break minutes", "number"],
    ["maxBreakMinutes", "Maximum break minutes", "number"],
    ["defaultMythicStart", "Default mythic start", "time"],
    ["postExtensionMinutes", "Post extension minutes", "number"],
  ] as const;
  return (
    <section
      className="card timing-card"
      aria-label="Calculation settings editor"
    >
      <h2>Calculation settings</h2>
      <p>
        All times are Pacific (America/Los_Angeles). Leave default mythic start
        empty for automatic detection.
      </p>
      <p>
        Save settings, then Run Pebble when ready. Saving does not recompute
        results.
      </p>
      <div role="status" aria-label="Calculation settings changes">
        {saving ? "Saving calculation settings…" : message}
      </div>
      {error && (
        <p className="field-error" role="alert">
          {error}
        </p>
      )}
      <fieldset disabled={blocked} className="timing-fields">
        <legend>Calculation parameters</legend>
        {fields.map(([key, label, type]) => (
          <div className="field" key={key}>
            <label>
              {label}
              <input
                type={type}
                step={
                  type === "number"
                    ? key === "postExtensionMinutes"
                      ? "any"
                      : "1"
                    : undefined
                }
                min={
                  type === "number"
                    ? key === "postExtensionMinutes"
                      ? 0
                      : 1
                    : undefined
                }
                max={
                  type === "number"
                    ? key === "postExtensionMinutes"
                      ? 60
                      : 120
                    : undefined
                }
                value={Number.isNaN(value[key]) ? "" : value[key]}
                aria-invalid={Boolean(validation)}
                aria-describedby={
                  validation ? "calculation-validation" : undefined
                }
                onChange={(event) =>
                  change((old) => ({
                    ...old,
                    [key]:
                      type === "number"
                        ? event.target.value === ""
                          ? NaN
                          : Number(event.target.value)
                        : event.target.value,
                  }))
                }
              />
            </label>
          </div>
        ))}
      </fieldset>
      {validation && (
        <p id="calculation-validation" className="field-error" role="alert">
          {validation}
        </p>
      )}
      <div className="card-action-row">
        <div className="button-row">
          <button
            className="button secondary"
            disabled={!dirty || blocked}
            onClick={cancel}
          >
            Cancel
          </button>
          <button
            className="button primary"
            disabled={!dirty || blocked || Boolean(validation)}
            onClick={save}
          >
            Save calculation settings
          </button>
        </div>
      </div>
    </section>
  );
}
