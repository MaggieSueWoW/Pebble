import { Check, CircleAlert, Info, RotateCcw } from "lucide-react";
import { SelectControl } from "../../components/SelectControl";
import { PanelHeader } from "../../components/PanelHeader";
import { type Player, type Availability, type Mode } from "../../shared/model";
import type { AvailabilityEditorState } from "./useAvailabilityEditor";
function AvailabilityField({
  label,
  value,
  onChange,
  max,
}: {
  label: string;
  value: Availability;
  onChange: (a: Availability) => void;
  max: number;
}) {
  const limit = value.mode === "unavailableFor" ? 720 : max;
  const helpId = `availability-${label.toLowerCase().replaceAll(" ", "-")}-help`;
  const invalid =
    !Number.isInteger(value.minutes) ||
    (value.minutes ?? 0) < 1 ||
    (value.minutes ?? 0) > limit;
  return (
    <div className="field">
      <label>
        {label}
        <SelectControl
          value={value.mode}
          onChange={(e) =>
            onChange(
              ["partial", "unavailableFor"].includes(e.target.value)
                ? { mode: e.target.value as Mode, minutes: max }
                : { mode: e.target.value as Mode },
            )
          }
        >
          <option value="automatic">Automatic</option>
          <option value="available">Available</option>
          <option value="unavailable">Unavailable</option>
          <option value="partial">Partial availability…</option>
          <option value="unavailableFor">Unavailable for…</option>
        </SelectControl>
      </label>
      {["partial", "unavailableFor"].includes(value.mode) && (
        <label className="minutes-label">
          {value.mode === "unavailableFor"
            ? "Unavailable minutes"
            : "Available minutes"}{" "}
          <input
            aria-label={`${label} ${value.mode === "unavailableFor" ? "unavailable" : "available"} minutes`}
            type="number"
            aria-invalid={invalid}
            aria-describedby={`${helpId}${invalid ? " availability-validation" : ""}`}
            min="1"
            max={limit}
            value={value.minutes ?? ""}
            onChange={(e) =>
              onChange({
                mode: value.mode,
                minutes:
                  e.target.value === "" ? undefined : Number(e.target.value),
              })
            }
          />
          <span id={helpId} className="muted small">
            {value.mode === "unavailableFor"
              ? "Pebble caps this adjustment to the raid half."
              : `Up to ${max} minutes in this half`}
          </span>
        </label>
      )}
    </div>
  );
}

export function AvailabilityEditor({
  player,
  editor,
  span,
}: {
  player: Player | undefined;
  editor: AvailabilityEditorState;
  span: { pre: number; post: number };
}) {
  if (!player)
    return (
      <section className="card adjustment-card empty-state">
        <h2>Select a player</h2>
        {editor.dirty || editor.saving ? (
          <>
            <p role="status">
              This player is no longer in the roster. Your edits are retained.
              {editor.saving
                ? " Wait for the save to finish."
                : " Cancel them or choose another player to continue."}
            </p>
            <button
              className="button secondary"
              disabled={editor.saving}
              onClick={editor.cancel}
            >
              Cancel edits
            </button>
          </>
        ) : (
          <p>Choose a current roster member to edit availability.</p>
        )}
      </section>
    );
  const p = player;
  const {
    value: edit,
    change: setEdit,
    dirty: editDirty,
    saving,
    blocked,
    validation: adjustmentError,
    error,
    message,
    cancel,
    reset,
    save,
  } = editor;
  return (
    <section
      className="card panel-body adjustment-card"
      aria-label="Player availability editor"
    >
      <div
        className="form-status"
        role="status"
        aria-label="Availability changes"
        aria-live="polite"
      >
        {saving ? "Saving availability…" : message}
      </div>
      {error && (
        <p role="alert" className="field-error">
          {error}
        </p>
      )}
      <PanelHeader
        title={<span style={{ color: p.color }}>{p.name}</span>}
        actions={editDirty && <span className="badge amber">Unsaved</span>}
      />
      <p className="editor-intro">
        Tell Pebble when this player was available. Automatic uses the bot’s
        normal inference.
      </p>
      <AvailabilityField
        label="Before break"
        value={edit.pre}
        max={span.pre}
        onChange={(pre) => setEdit((old) => ({ ...old, pre }))}
      />
      <AvailabilityField
        label="After break"
        value={edit.post}
        max={span.post}
        onChange={(post) => setEdit((old) => ({ ...old, post }))}
      />
      <div className="field">
        <label>
          Notes <span className="optional">optional · private</span>
          <textarea
            rows={2}
            maxLength={500}
            placeholder="Anything helpful to remember…"
            aria-label="Availability notes"
            value={edit.notes}
            onChange={(e) =>
              setEdit((old) => ({
                ...old,
                notes: e.target.value,
              }))
            }
          />
        </label>
      </div>
      {adjustmentError && (
        <p id="availability-validation" role="alert" className="field-error">
          <CircleAlert size={14} />
          {adjustmentError}
        </p>
      )}
      <div className="editor-actions">
        <button
          className="button primary"
          onClick={save}
          disabled={!editDirty || Boolean(adjustmentError) || blocked}
        >
          <Check size={15} /> Save adjustment
        </button>
        <button
          className="button secondary"
          disabled={!editDirty || editor.saving}
          onClick={cancel}
        >
          Cancel
        </button>
      </div>
      <button
        className="text-button reset-link"
        disabled={blocked}
        onClick={reset}
      >
        <RotateCcw size={13} /> Reset to automatic
      </button>
      <div className="notice">
        <Info size={16} />
        <span>
          Save first, then Run Pebble. Current results stay the same until the
          run completes.
        </span>
      </div>
    </section>
  );
}
