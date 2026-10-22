import { calculationFields } from "./calculationFields";
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
  return (
    <section
      className="card timing-card calculation-card"
      aria-label="Calculation settings editor"
    >
      <p className="settings-form-intro">
        All times are Pacific. Save, then Run Pebble to apply changes.
      </p>
      <div role="status" aria-label="Calculation settings changes">
        {saving ? "Saving calculation settings…" : message}
      </div>
      {error && (
        <p className="field-error" role="alert">
          {error}
        </p>
      )}
      <fieldset
        disabled={blocked}
        className="calculation-fields"
        aria-label="Calculation settings"
      >
        {calculationFields.map(({ key, label, type, description }) => (
          <div className="field calculation-field" key={key}>
            <label htmlFor={`calculation-${key}`}>{label}</label>
            <input
              id={`calculation-${key}`}
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
              aria-describedby={`calculation-${key}-description${validation ? " calculation-validation" : ""}`}
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
            <p
              className="calculation-description"
              id={`calculation-${key}-description`}
            >
              {description}
            </p>
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
            Save
          </button>
        </div>
      </div>
    </section>
  );
}
