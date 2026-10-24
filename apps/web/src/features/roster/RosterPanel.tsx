import { PanelHeader } from "../../components/PanelHeader";
import { useState } from "react";
import { Trash2 } from "lucide-react";
import { SelectControl } from "../../components/SelectControl";
import { classes, type RosterValue } from "./roster";
import type { RosterEditorState } from "./useRosterEditor";
export function RosterPanel({
  editor,
  remove,
  cancelCreate,
  linkedAlts,
  linkedOverrides,
  deletionBlocked,
}: {
  editor: RosterEditorState;
  remove: () => Promise<void>;
  cancelCreate: () => void;
  linkedAlts: number;
  linkedOverrides: number;
  deletionBlocked: boolean;
}) {
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState("");
  const field = (key: keyof RosterValue, value: string | boolean) =>
    editor.change((old) => ({ ...old, [key]: value }));
  return (
    <section
      className="panel panel-body roster-panel"
      aria-label="Main details"
    >
      <PanelHeader
        title={editor.exists ? "Main" : "New main"}
        id="roster-title"
      />
      <div className="roster-editor">
        {editor.id && (
          <form
            onSubmit={(e) => {
              e.preventDefault();
              void editor.save();
            }}
          >
            <fieldset disabled={editor.blocked} className="roster-fields">
              <legend className="sr-only">
                {editor.exists ? "Edit main" : "New main"}
              </legend>
              <label className="field">
                Main name
                <input
                  value={editor.value.name}
                  maxLength={32}
                  onChange={(e) => field("name", e.target.value)}
                  aria-describedby="roster-validation"
                />
              </label>
              <label className="field">
                Class
                <SelectControl
                  aria-label="Class"
                  value={editor.value.className}
                  onChange={(e) => field("className", e.target.value)}
                >
                  {classes.map((v) => (
                    <option key={v}>{v}</option>
                  ))}
                </SelectControl>
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
              <label className="field roster-active">
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
                  Inactive players stay in the roster.
                </span>
              </label>
              <label className="field">
                Private roster notes
                <textarea
                  rows={2}
                  value={editor.value.notes}
                  maxLength={1000}
                  onChange={(e) => field("notes", e.target.value)}
                />
              </label>
            </fieldset>
            <p
              className="validation"
              id="roster-validation"
              role={editor.validation && editor.dirty ? "alert" : undefined}
            >
              {editor.dirty ? editor.validation : ""}
            </p>
            {editor.error && <p role="alert">{editor.error}</p>}
            <div
              className="form-status"
              role="status"
              aria-label="Roster changes"
            >
              {editor.message}
            </div>
            <p className="muted small roster-help">
              Changes apply after Run Pebble.
            </p>
            <div className="button-row roster-main-actions">
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
                aria-label="Cancel roster edits"
                type="button"
                disabled={(editor.exists && !editor.dirty) || editor.blocked}
                onClick={() => {
                  editor.cancel();
                  if (!editor.exists) cancelCreate();
                }}
              >
                Cancel
              </button>
              {editor.exists && !confirmDelete && (
                <button
                  className="text-button delete-main-button"
                  type="button"
                  disabled={
                    editor.dirty ||
                    editor.blocked ||
                    deleting ||
                    deletionBlocked
                  }
                  onClick={() => setConfirmDelete(true)}
                >
                  <Trash2 size={14} /> Delete main
                </button>
              )}
            </div>
            {confirmDelete && (
              <div
                className="delete-main-confirm"
                role="group"
                aria-label="Confirm main deletion"
              >
                <p>
                  Delete <strong>{editor.value.name}</strong>
                  {linkedAlts || linkedOverrides
                    ? `, ${linkedAlts} alt ${linkedAlts === 1 ? "mapping" : "mappings"}, and ${linkedOverrides} availability ${linkedOverrides === 1 ? "override" : "overrides"}`
                    : ""}
                  ? This cannot be undone. Completed results stay until Run
                  Pebble.
                </p>
                {deleteError && <p role="alert">{deleteError}</p>}
                <div className="button-row">
                  <button
                    className="button danger"
                    type="button"
                    disabled={
                      deleting ||
                      editor.blocked ||
                      editor.dirty ||
                      deletionBlocked
                    }
                    onClick={async () => {
                      setDeleting(true);
                      setDeleteError("");
                      try {
                        await remove();
                      } catch (cause) {
                        setDeleteError(
                          cause instanceof Error
                            ? cause.message
                            : "Could not delete this main.",
                        );
                      } finally {
                        setDeleting(false);
                      }
                    }}
                  >
                    {deleting ? "Deleting…" : "Confirm delete main"}
                  </button>
                  <button
                    type="button"
                    className="button secondary"
                    disabled={deleting}
                    onClick={() => setConfirmDelete(false)}
                  >
                    Keep main
                  </button>
                </div>
              </div>
            )}
          </form>
        )}
        {!editor.id && (
          <p>No mains in the roster. Add the first main to get started.</p>
        )}
      </div>
    </section>
  );
}
