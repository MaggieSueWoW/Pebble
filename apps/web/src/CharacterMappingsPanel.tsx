import { PanelHeader } from "./PanelHeader";
import { useState } from "react";
import { ArrowLeftRight, Plus, Trash2 } from "lucide-react";
import {
  regions,
  identityCasefold,
  type CharacterEntry,
  type CharacterValue,
} from "./characters";
import { SelectControl } from "./SelectControl";
import type { CharacterEditorState } from "./useCharacterEditor";
export function CharacterMappingsPanel({
  characters,
  playerId,
  editor,
  editing,
  edit,
  add,
  remove,
  cancelAdd,
  swap,
  mainName,
  mainClass,
  actionsBlocked,
}: {
  characters: CharacterEntry[];
  playerId: string;
  editor: CharacterEditorState;
  editing: boolean;
  edit: (id: string) => void;
  add: () => void;
  remove: (id: string, version: number) => Promise<void>;
  cancelAdd: () => void;
  swap: (id: string, version: number) => Promise<void>;
  mainName: string;
  mainClass: string;
  actionsBlocked: boolean;
}) {
  const [swapId, setSwapId] = useState<string | null>(null);
  const [confirmId, setConfirmId] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState("");
  const alts = characters
    .filter(
      (alt) =>
        alt.playerId === playerId &&
        identityCasefold(alt.name) !== identityCasefold(mainName),
    )
    .sort((a, b) => a.name.localeCompare(b.name));
  const field = (key: keyof CharacterValue, value: string) =>
    editor.change((old) => ({ ...old, [key]: value }));
  return (
    <section
      className="panel panel-body roster-panel roster-alts-panel"
      aria-labelledby="character-title"
    >
      <PanelHeader
        title="Alts"
        id="character-title"
        actions={
          <button
            className="text-button"
            onClick={add}
            disabled={deleting || editor.saving}
          >
            <Plus size={14} /> Add alt
          </button>
        }
      />
      <div className="roster-editor">
        <ul className="main-alt-list">
          {alts.map((alt) => (
            <li key={alt.id}>
              <div className="main-alt-row">
                <button
                  className="roster-alt-button"
                  onClick={() => edit(alt.id)}
                >
                  {alt.name}
                </button>
                <span className="muted small">
                  {alt.realm || "All realms"} · {alt.region.toUpperCase()}
                </span>
                <button
                  className="swap-main-button"
                  aria-label={`Make ${alt.name} main`}
                  title={`Swap ${alt.name} with ${mainName}`}
                  disabled={
                    deleting ||
                    editor.dirty ||
                    editor.blocked ||
                    actionsBlocked ||
                    alt.name.toLocaleLowerCase() ===
                      mainName.toLocaleLowerCase()
                  }
                  onClick={() => {
                    setSwapId(alt.id);
                    setConfirmId(null);
                    setDeleteError("");
                  }}
                >
                  <ArrowLeftRight size={14} /> Make main
                </button>
                <button
                  className="delete-alt-button"
                  aria-label={`Delete alt ${alt.name}`}
                  title={`Delete alt ${alt.name}`}
                  disabled={
                    deleting || editor.dirty || editor.blocked || actionsBlocked
                  }
                  onClick={() => {
                    setConfirmId(alt.id);
                    setSwapId(null);
                    setDeleteError("");
                  }}
                >
                  <Trash2 size={14} />
                </button>
              </div>
              {swapId === alt.id && (
                <div
                  className="swap-main-confirm"
                  role="group"
                  aria-label="Confirm main swap"
                >
                  <p>
                    Make <strong>{alt.name}</strong> the main and move{" "}
                    <strong>{mainName}</strong> to alts? Class stays{" "}
                    {mainClass || "Unknown"}; membership and history stay with
                    this player.
                  </p>

                  {deleteError && <p role="alert">{deleteError}</p>}
                  <button
                    className="text-button"
                    disabled={
                      deleting ||
                      editor.blocked ||
                      editor.dirty ||
                      actionsBlocked
                    }
                    onClick={async () => {
                      setDeleting(true);
                      setDeleteError("");
                      try {
                        await swap(alt.id, alt.version);
                        setSwapId(null);
                      } catch (cause) {
                        setDeleteError(
                          cause instanceof Error
                            ? cause.message
                            : "Could not swap the main and alt.",
                        );
                      } finally {
                        setDeleting(false);
                      }
                    }}
                  >
                    {deleting ? "Swapping…" : "Swap main and alt"}
                  </button>
                  <button
                    className="text-button"
                    disabled={deleting}
                    onClick={() => setSwapId(null)}
                  >
                    Keep current main
                  </button>
                </div>
              )}
              {confirmId === alt.id && (
                <div
                  className="delete-alt-confirm"
                  role="group"
                  aria-label={`Confirm deletion of ${alt.name}`}
                >
                  <p>Delete {alt.name} from this main?</p>
                  {deleteError && <p role="alert">{deleteError}</p>}
                  <button
                    className="button danger compact"
                    disabled={
                      deleting ||
                      editor.blocked ||
                      editor.dirty ||
                      actionsBlocked
                    }
                    onClick={async () => {
                      setDeleting(true);
                      setDeleteError("");
                      try {
                        await remove(alt.id, alt.version);
                        setConfirmId(null);
                      } catch (cause) {
                        setDeleteError(
                          cause instanceof Error
                            ? cause.message
                            : "Could not delete this alt.",
                        );
                      } finally {
                        setDeleting(false);
                      }
                    }}
                  >
                    {deleting ? "Deleting…" : "Confirm delete alt"}
                  </button>
                  <button
                    className="button secondary compact"
                    disabled={deleting}
                    onClick={() => setConfirmId(null)}
                  >
                    Keep alt
                  </button>
                </div>
              )}
            </li>
          ))}
        </ul>
        {!alts.length && <p className="muted small alt-empty">No alts yet.</p>}
        {editing && editor.id && (
          <form
            onSubmit={(e) => {
              e.preventDefault();
              void editor.save();
            }}
          >
            <p className="alt-form-title">
              {editor.exists ? `Edit ${editor.value.name || "alt"}` : "New alt"}
            </p>
            <fieldset className="roster-fields" disabled={editor.blocked}>
              <legend className="sr-only">
                {editor.exists ? "Edit alt" : "New alt"}
              </legend>
              <label className="field">
                Character name
                <input
                  value={editor.value.name}
                  maxLength={32}
                  aria-describedby="character-match character-validation"
                  onChange={(e) => field("name", e.target.value)}
                />
              </label>
              <label className="field">
                Realm
                <input
                  value={editor.value.realm}
                  maxLength={100}
                  aria-describedby="character-match character-validation"
                  onChange={(e) => field("realm", e.target.value)}
                />
              </label>
              <label className="field">
                Region
                <SelectControl
                  aria-label="Region"
                  value={editor.value.region}
                  onChange={(e) => field("region", e.target.value)}
                >
                  {regions.map((region) => (
                    <option key={region} value={region}>
                      {region.toUpperCase()}
                    </option>
                  ))}
                </SelectControl>
              </label>
              <p className="muted small roster-help" id="character-match">
                Leave realm blank to match this name on any realm. Changes apply
                after Run Pebble.
              </p>
            </fieldset>
            <p
              className="validation"
              id="character-validation"
              role={editor.validation && editor.dirty ? "alert" : undefined}
            >
              {editor.dirty ? editor.validation : ""}
            </p>
            {editor.error && <p role="alert">{editor.error}</p>}
            <div
              className="form-status"
              role="status"
              aria-label="Character mapping changes"
            >
              {editor.message}
            </div>
            <div className="button-row">
              <button
                type="submit"
                className="button primary"
                disabled={
                  !editor.dirty || Boolean(editor.validation) || editor.blocked
                }
              >
                {editor.saving ? "Saving…" : "Save alt"}
              </button>
              <button
                className="button secondary"
                type="button"
                disabled={(editor.exists && !editor.dirty) || editor.blocked}
                onClick={() => {
                  editor.cancel();
                  if (!editor.exists) cancelAdd();
                }}
              >
                Cancel alt edits
              </button>
            </div>
          </form>
        )}
      </div>
    </section>
  );
}
