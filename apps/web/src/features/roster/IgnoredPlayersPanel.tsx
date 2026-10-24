import { useState } from "react";
import { Search, Undo2 } from "lucide-react";
import { DataTable } from "../../components/DataTable";
import { PanelHeader } from "../../components/PanelHeader";
import type { LiveConnection } from "../../data/useFirestore";
import type { CharacterEntry } from "./characters";

export function IgnoredPlayersPanel({
  connection,
}: {
  connection: LiveConnection;
}) {
  const [search, setSearch] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const query = search.trim().toLocaleLowerCase();
  const characters = [...connection.ignoredCharacters]
    .filter((character) =>
      [character.name, character.realm, character.region].some((value) =>
        value.toLocaleLowerCase().includes(query),
      ),
    )
    .sort(
      (a, b) =>
        a.name.localeCompare(b.name) ||
        a.realm.localeCompare(b.realm) ||
        a.region.localeCompare(b.region),
    );
  async function undo(character: CharacterEntry) {
    setSaving(true);
    setError("");
    setMessage("");
    try {
      await connection.undoIgnoreCharacter(character.id, character.version);
      setMessage(
        `${character.name}: ignore removed. Run Pebble to update warnings and results.`,
      );
    } catch (cause) {
      setError(
        cause instanceof Error ? cause.message : "Could not undo this ignore.",
      );
    } finally {
      setSaving(false);
    }
  }
  return (
    <section
      className="card panel-body"
      aria-labelledby="ignored-players-title"
    >
      <PanelHeader
        id="ignored-players-title"
        title={`Ignored players (${connection.ignoredCharacters.length})`}
        description={`Season: ${connection.label}. Undo an ignore to review the character again on the next Pebble run.`}
      />
      <label className="search-field">
        <Search size={16} />
        <input
          aria-label="Search ignored players"
          placeholder="Find a name, realm, or region…"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
        />
      </label>
      {error && <p role="alert">{error}</p>}
      <p role="status">{message}</p>
      {characters.length ? (
        <DataTable>
          <thead>
            <tr>
              <th scope="col">Character</th>
              <th scope="col">Realm</th>
              <th scope="col">Region</th>
              <th scope="col">
                <span className="sr-only">Actions</span>
              </th>
            </tr>
          </thead>
          <tbody>
            {characters.map((character) => (
              <tr key={character.id}>
                <td>{character.name}</td>
                <td>{character.realm || "All realms"}</td>
                <td>{character.region.toUpperCase()}</td>
                <td>
                  <button
                    className="text-button"
                    aria-label={`Undo ignore for ${character.name} (${character.realm || "all realms"}, ${character.region.toUpperCase()})`}
                    disabled={saving || connection.busy || !connection.officer}
                    onClick={() => void undo(character)}
                  >
                    <Undo2 size={14} /> Undo ignore
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </DataTable>
      ) : (
        <p className="empty-state">
          {connection.ignoredCharacters.length
            ? "No matching ignored players."
            : "No ignored players this season."}
        </p>
      )}
    </section>
  );
}
