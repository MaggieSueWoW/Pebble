import type { LiveConnection } from "../../data/useFirestore";
import { SelectControl } from "../../components/SelectControl";
import type { MissingPlayer } from "../results/resultProvenance";
import { characterIdentity } from "../roster/characters";
import { classes } from "../roster/roster";
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
      className="card panel-body missing-players-card"
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
      <div
        className="missing-player-list"
        role="region"
        aria-label="Missing player actions"
        tabIndex={0}
      >
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
          const status = main
            ? `Main added · joins ${main.joinDate || "all dates"}. Run Pebble to refresh.`
            : mapped
              ? `Mapped to ${roster.find((item) => item.id === mapped.playerId)?.name ?? "a main"}. Run Pebble to refresh.`
              : "Ignored for this season. Run Pebble to refresh.";
          return (
            <article
              className="missing-player"
              key={key}
              aria-label={`Missing player ${player.name}`}
            >
              <div className="missing-player-heading">
                <strong>{player.name}</strong>
                <span
                  className="muted small"
                  title={[player.realm, player.region.toUpperCase()]
                    .filter(Boolean)
                    .join(" · ")}
                >
                  {[
                    player.realm,
                    player.region.toUpperCase() || "Region unavailable",
                  ]
                    .filter(Boolean)
                    .join(" · ")}
                </span>
              </div>
              <div className="missing-player-controls">
                {resolved && !draft ? (
                  <span className="muted missing-player-status" title={status}>
                    {status}
                  </span>
                ) : draft ? (
                  <div className="missing-player-form">
                    {draft.mode === "main" ? (
                      <>
                        <label>
                          Class:{" "}
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
                        <span className="muted small">
                          Join date: {draft.night}
                        </span>
                      </>
                    ) : draft.mode === "alt" ? (
                      <label>
                        Main:{" "}
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
                    ) : (
                      <span>Ignore for this season?</span>
                    )}
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
                          : "Confirm ignore"}
                    </button>
                    <button
                      className="button secondary compact"
                      aria-label="Cancel player change"
                      disabled={editor.saving}
                      onClick={editor.cancel}
                    >
                      Cancel
                    </button>
                  </div>
                ) : (
                  <div className="missing-player-actions">
                    <button
                      className="button secondary compact"
                      disabled={
                        editor.saving || connection.busy || !identityReady
                      }
                      title={
                        !identityReady
                          ? "Refresh the report region before ignoring this player."
                          : undefined
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
                      title={
                        !identityReady
                          ? "Refresh the report region before mapping this player."
                          : undefined
                      }
                      onClick={() => onAction(player, "alt")}
                    >
                      Add alt
                    </button>
                  </div>
                )}
              </div>
            </article>
          );
        })}
      </div>
    </section>
  );
}
