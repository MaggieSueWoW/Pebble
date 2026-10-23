import type { LiveConnection } from "../../data/useFirestore";
import { SelectControl } from "../../components/SelectControl";
import type { MissingPlayer } from "../results/resultProvenance";
import { characterIdentity } from "../roster/characters";
import { classes, roles } from "../roster/roster";
import type { MissingPlayerEditor, Resolution } from "./useMissingPlayerEditor";

export function MissingPlayers({
  players,
  connection,
  editor,
  onAction,
}: {
  players: MissingPlayer[];
  connection: LiveConnection;
  editor: MissingPlayerEditor;
  onAction: (player: MissingPlayer, mode: Resolution) => void;
}) {
  const roster = [...connection.roster].sort((a, b) =>
    a.name.localeCompare(b.name),
  );
  return (
    <section
      className="card panel-body"
      aria-labelledby="missing-players-title"
    >
      <h2 id="missing-players-title">Missing players</h2>
      <div role="status" aria-label="Player resolution">
        {editor.saving ? "Saving player…" : editor.message}
      </div>
      {editor.error && (
        <p role="alert" className="field-error">
          {editor.error}
        </p>
      )}
      {!players.length && <p>No missing players.</p>}
      {players.map((player) => {
        const key = characterIdentity({ ...player, playerId: "" });
        const ignored = connection.ignoredCharacters.some(
          (item) => characterIdentity(item) === key,
        );
        const mapped = connection.characters.find(
          (item) => characterIdentity(item) === key,
        );
        const main = roster.find(
          (item) => item.name.toLowerCase() === player.name.toLowerCase(),
        );
        const resolved = main || mapped || ignored;
        const draft =
          editor.draft &&
          characterIdentity({ ...editor.draft.player, playerId: "" }) === key
            ? editor.draft
            : null;
        const identityReady = ["us", "eu", "kr", "tw", "cn"].includes(
          player.region,
        );
        return (
          <article
            className="missing-player"
            key={key}
            aria-label={`Missing player ${player.name}`}
          >
            <div className="missing-player-heading">
              <strong>{player.name}</strong>
              <span className="muted small">
                {[player.realm, player.region.toUpperCase()]
                  .filter(Boolean)
                  .join(" · ")}
              </span>
            </div>
            {resolved ? (
              <p className="muted">
                {main
                  ? `Main added · joins ${main.joinDate || "all dates"}.`
                  : mapped
                    ? `Mapped to ${roster.find((item) => item.id === mapped.playerId)?.name ?? "a main"}.`
                    : "Ignored for this season."}{" "}
                Run Pebble to refresh.
              </p>
            ) : (
              <>
                {!draft && (
                  <div className="button-row missing-player-actions">
                    <button
                      className="button secondary compact"
                      disabled={
                        editor.saving || connection.busy || !identityReady
                      }
                      onClick={() => onAction(player, "ignore")}
                    >
                      Ignore forever
                    </button>
                    <button
                      className="button secondary compact"
                      disabled={editor.saving || connection.busy}
                      onClick={() => onAction(player, "main")}
                    >
                      Add main
                    </button>
                    <button
                      className="button secondary compact"
                      disabled={
                        editor.saving ||
                        connection.busy ||
                        !roster.length ||
                        !identityReady
                      }
                      onClick={() => onAction(player, "alt")}
                    >
                      Add alt
                    </button>
                  </div>
                )}
                {!identityReady && (
                  <p className="muted small">
                    Report region unavailable. Refresh the report data before
                    ignoring or mapping an alt.
                  </p>
                )}
                {draft && (
                  <div className="missing-player-form">
                    {draft.mode === "main" ? (
                      <>
                        <p>Join date: {draft.night}</p>
                        <label>
                          Class
                          <SelectControl
                            aria-label={`Class for ${player.name}`}
                            value={draft.className}
                            disabled={editor.saving}
                            onChange={(event) =>
                              editor.change({ className: event.target.value })
                            }
                          >
                            {classes.map((name) => (
                              <option key={name}>{name}</option>
                            ))}
                          </SelectControl>
                        </label>
                        <label>
                          Role
                          <SelectControl
                            aria-label={`Role for ${player.name}`}
                            value={draft.role}
                            disabled={editor.saving}
                            onChange={(event) =>
                              editor.change({ role: event.target.value })
                            }
                          >
                            {roles.map((name) => (
                              <option key={name}>{name}</option>
                            ))}
                          </SelectControl>
                        </label>
                      </>
                    ) : draft.mode === "alt" ? (
                      <label>
                        Main
                        <SelectControl
                          aria-label={`Main for ${player.name}`}
                          value={draft.mainId}
                          disabled={editor.saving}
                          onChange={(event) =>
                            editor.change({ mainId: event.target.value })
                          }
                        >
                          <option value="">Choose a main</option>
                          {roster.map((item) => (
                            <option key={item.id} value={item.id}>
                              {item.name}
                            </option>
                          ))}
                        </SelectControl>
                      </label>
                    ) : null}
                    <div className="button-row">
                      <button
                        className="button secondary compact"
                        disabled={editor.saving}
                        onClick={editor.cancel}
                      >
                        Cancel player change
                      </button>
                      <button
                        className="button primary compact"
                        disabled={
                          editor.saving ||
                          connection.busy ||
                          (draft.mode === "alt" && !draft.mainId)
                        }
                        onClick={() => void editor.save()}
                      >
                        {draft.mode === "main"
                          ? "Save main"
                          : draft.mode === "alt"
                            ? "Save alt"
                            : "Retry ignore"}
                      </button>
                    </div>
                  </div>
                )}
              </>
            )}
          </article>
        );
      })}
    </section>
  );
}
