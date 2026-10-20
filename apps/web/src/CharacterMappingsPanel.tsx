import {
  regions,
  type CharacterEntry,
  type CharacterValue,
} from "./characters";
import type { RosterEntry } from "./roster";
import type { CharacterEditorState } from "./useCharacterEditor";
export function CharacterMappingsPanel({
  characters,
  roster,
  editor,
  select,
  add,
}: {
  characters: CharacterEntry[];
  roster: RosterEntry[];
  editor: CharacterEditorState;
  select: (id: string) => void;
  add: () => void;
}) {
  const field = (key: keyof CharacterValue, value: string) =>
    editor.change((old) => ({ ...old, [key]: value }));
  return (
    <section className="panel roster-panel" aria-labelledby="character-title">
      <div className="panel-header">
        <div>
          <h2 id="character-title">Character mappings</h2>
          <p className="muted">
            Connect logged characters and alts to their roster main.
          </p>
        </div>
        <button
          className="button secondary"
          onClick={add}
          disabled={editor.saving || !roster.length}
        >
          Add mapping
        </button>
      </div>
      <div className="roster-editor">
        {!roster.length && (
          <p>Add a roster main before creating character mappings.</p>
        )}
        <label className="field">
          Character mapping
          <select
            aria-label="Character mapping"
            value={editor.exists ? editor.id : ""}
            disabled={editor.blocked}
            onChange={(e) => select(e.target.value)}
          >
            <option value="" disabled>
              {characters.length ? "New mapping" : "No mappings yet"}
            </option>
            {characters.map((character) => (
              <option key={character.id} value={character.id}>
                {character.name} · {character.realm || "All realms"} ·{" "}
                {character.region.toUpperCase()}
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
            <fieldset className="roster-fields" disabled={editor.blocked}>
              <legend>{editor.exists ? "Edit mapping" : "New mapping"}</legend>
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
                <select
                  aria-label="Region"
                  value={editor.value.region}
                  onChange={(e) => field("region", e.target.value)}
                >
                  {regions.map((region) => (
                    <option key={region} value={region}>
                      {region.toUpperCase()}
                    </option>
                  ))}
                </select>
              </label>
              <p className="muted small" id="character-match">
                Names and realms match without case sensitivity; surrounding
                spaces are trimmed when saved. An empty realm matches this name
                on every realm in the selected region. A specific realm takes
                precedence.
              </p>
              <label className="field">
                Mapped main
                <select
                  aria-label="Mapped main"
                  value={editor.value.playerId}
                  aria-describedby="character-effects character-validation"
                  onChange={(e) => field("playerId", e.target.value)}
                >
                  <option value="">Choose a main</option>
                  {!roster.some(
                    (player) => player.id === editor.value.playerId,
                  ) &&
                    editor.value.playerId && (
                      <option value={editor.value.playerId}>
                        Missing main — choose another
                      </option>
                    )}
                  {roster.map((player) => (
                    <option key={player.id} value={player.id}>
                      {player.name}
                      {player.active ? "" : " (inactive)"}
                    </option>
                  ))}
                </select>
              </label>
              <p className="muted small" id="character-effects">
                Editing keeps this mapping’s ID. Changing the name, realm or
                region replaces its old match; changing the main moves matched
                logs to that main on the next run. Inactive mains remain
                available, but do not appear in active results. Completed
                results stay unchanged until Run Pebble. Mappings cannot be
                deleted here; edit an existing mapping to correct it.
              </p>
            </fieldset>
            <p
              id="character-validation"
              role={editor.validation && editor.dirty ? "alert" : undefined}
            >
              {editor.dirty ? editor.validation : ""}
            </p>
            {editor.error && <p role="alert">{editor.error}</p>}
            <div role="status" aria-label="Character mapping changes">
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
                {editor.saving ? "Saving…" : "Save mapping"}
              </button>
              <button
                className="button secondary"
                type="button"
                disabled={!editor.dirty || editor.blocked}
                onClick={editor.cancel}
              >
                Cancel mapping edits
              </button>
            </div>
          </form>
        )}
        {!editor.id && (
          <p>
            No character mappings yet. Unmapped characters use main-name
            matching.
          </p>
        )}
      </div>
    </section>
  );
}
