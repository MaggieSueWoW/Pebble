import { classes, roles, type RosterEntry, type RosterValue } from "./roster";
import type { RosterEditorState } from "./useRosterEditor";
export function RosterPanel({
  roster,
  editor,
  select,
  add,
}: {
  roster: RosterEntry[];
  editor: RosterEditorState;
  select: (id: string) => void;
  add: () => void;
}) {
  const field = (key: keyof RosterValue, value: string | boolean) =>
    editor.change((old) => ({ ...old, [key]: value }));
  return (
    <section className="panel roster-panel" aria-labelledby="roster-title">
      <div className="panel-header">
        <div>
          <h2 id="roster-title">Manage roster</h2>
          <p className="muted">
            Add mains or update membership. Save, then Run Pebble to update
            results.
          </p>
        </div>
        <button
          className="button secondary"
          onClick={add}
          disabled={editor.saving}
        >
          Add main
        </button>
      </div>
      <div className="roster-editor">
        <label className="field">
          Main player
          <select
            aria-label="Main player"
            value={editor.exists ? editor.id : ""}
            onChange={(e) => select(e.target.value)}
            disabled={editor.blocked}
          >
            <option value="" disabled>
              {roster.length ? "New main" : "No mains yet"}
            </option>
            {roster.map((p) => (
              <option key={p.id} value={p.id}>
                {p.name}
                {p.active ? "" : " (inactive)"}
              </option>
            ))}
          </select>
        </label>
        {editor.id && (
          <form
            onSubmit={(e) => {
              e.preventDefault();
              void editor.save();
            }}
          >
            <fieldset disabled={editor.blocked} className="roster-fields">
              <legend>{editor.exists ? "Edit main" : "New main"}</legend>
              <label className="field">
                Main name
                <input
                  value={editor.value.name}
                  maxLength={32}
                  onChange={(e) => field("name", e.target.value)}
                  aria-describedby="roster-identity roster-validation"
                />
              </label>
              <p id="roster-identity" className="muted small">
                Renaming keeps the same player ID and character mappings.
                Unmapped logs match the main name; map old names and alts before
                recomputing. Completed results change only after a new run.
              </p>
              <label className="field">
                Class
                <select
                  aria-label="Class"
                  value={editor.value.className}
                  onChange={(e) => field("className", e.target.value)}
                >
                  {classes.map((v) => (
                    <option key={v}>{v}</option>
                  ))}
                </select>
              </label>
              <label className="field">
                Role
                <select
                  aria-label="Role"
                  value={editor.value.role}
                  onChange={(e) => field("role", e.target.value)}
                >
                  {roles.map((v) => (
                    <option key={v}>{v}</option>
                  ))}
                </select>
              </label>
              <label className="field">
                <span>
                  <input
                    type="checkbox"
                    aria-label="Active"
                    checked={editor.value.active}
                    onChange={(e) => field("active", e.target.checked)}
                  />{" "}
                  Active
                </span>
                <span className="muted small">
                  Uncheck to archive; no player or mapping is deleted. Dates
                  apply to historical membership.
                </span>
              </label>
              <label className="field">
                Join date
                <input
                  type="date"
                  value={editor.value.joinDate}
                  aria-describedby="roster-validation"
                  onChange={(e) => field("joinDate", e.target.value)}
                />
              </label>
              <label className="field">
                Leave date
                <input
                  type="date"
                  value={editor.value.leaveDate}
                  aria-describedby="roster-validation"
                  onChange={(e) => field("leaveDate", e.target.value)}
                />
              </label>
              <label className="field">
                Private roster notes
                <textarea
                  value={editor.value.notes}
                  maxLength={1000}
                  onChange={(e) => field("notes", e.target.value)}
                />
              </label>
            </fieldset>
            <p
              id="roster-validation"
              role={editor.validation && editor.dirty ? "alert" : undefined}
            >
              {editor.dirty ? editor.validation : ""}
            </p>
            {editor.error && <p role="alert">{editor.error}</p>}
            <div role="status" aria-label="Roster changes">
              {editor.message}
            </div>
            <div className="button-row">
              <button
                className="button primary"
                type="submit"
                disabled={
                  !editor.dirty || Boolean(editor.validation) || editor.blocked
                }
              >
                {editor.saving ? "Saving…" : "Save main"}
              </button>
              <button
                className="button secondary"
                type="button"
                disabled={!editor.dirty || editor.blocked}
                onClick={editor.cancel}
              >
                Cancel roster edits
              </button>
            </div>
          </form>
        )}
        {!editor.id && (
          <p>No mains in the roster. Add the first main to get started.</p>
        )}
      </div>
    </section>
  );
}
