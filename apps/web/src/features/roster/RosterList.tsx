import { Fragment, useState, type ReactNode } from "react";
import { ChevronDown, ChevronRight, Plus, Search } from "lucide-react";
import { DataTable } from "../../components/DataTable";
import { SortButton } from "../../components/SortButton";
import type { RosterEntry } from "./roster";
import { identityCasefold } from "./characters";
import type { CharacterEntry } from "./characters";

export function RosterList({
  roster,
  characters,
  colors,
  editMain,
  addMain,
  editAlt,
  detail,
  detailPlayerId,
  close,
}: {
  detail?: ReactNode;
  detailPlayerId?: string;
  close: () => void;
  roster: RosterEntry[];
  characters: CharacterEntry[];
  colors: Record<string, string>;
  editMain: (id: string) => void;
  addMain: () => void;
  editAlt: (id: string) => void;
}) {
  const [search, setSearch] = useState("");
  const [sort, setSort] = useState<{
    key: "name" | "joinDate" | "leaveDate" | "alts";
    direction: "ascending" | "descending";
  }>({ key: "name", direction: "ascending" });
  const visibleAlts = new Map(
    roster.map((player) => [
      player.id,
      characters
        .filter(
          (alt) =>
            alt.playerId === player.id &&
            identityCasefold(alt.name) !== identityCasefold(player.name),
        )
        .sort(
          (a, b) =>
            a.name.localeCompare(b.name, "en", { sensitivity: "base" }) ||
            a.id.localeCompare(b.id),
        ),
    ]),
  );
  const sortValue = (player: RosterEntry) =>
    sort.key === "alts"
      ? visibleAlts
          .get(player.id)!
          .map((alt) => alt.name)
          .join(", ")
      : player[sort.key];
  const query = search.trim().toLocaleLowerCase();
  const players = [...roster]
    .sort((a, b) => {
      const byName =
        a.name.localeCompare(b.name, "en", { sensitivity: "base" }) ||
        a.id.localeCompare(b.id);
      const left = sortValue(a),
        right = sortValue(b);
      // Empty dates and alt lists stay last in either direction.
      if (!left || !right) return left ? -1 : right ? 1 : byName;
      const compared = left.localeCompare(right, "en", { sensitivity: "base" });
      return (sort.direction === "ascending" ? compared : -compared) || byName;
    })
    .filter(
      (player) =>
        player.id === detailPlayerId ||
        [
          player.name,
          player.className,
          ...characters
            .filter((alt) => alt.playerId === player.id)
            .map((alt) => alt.name),
        ].some((value) => value.toLocaleLowerCase().includes(query)),
    );
  const toggle = (id: string) =>
    detail && detailPlayerId === id ? close() : editMain(id);
  return (
    <section className="card roster-list-card" aria-label="Roster players">
      <div className="roster-list-toolbar">
        <label className="search-field">
          <Search size={16} />
          <input
            aria-label="Search roster"
            placeholder="Find a player, class, or alt…"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
          />
        </label>
        <button className="button secondary" onClick={addMain}>
          <Plus size={14} /> Add main
        </button>
      </div>
      {detail &&
        !roster.some((player) => player.id === detailPlayerId) &&
        detail}
      <DataTable className="roster-table">
        <thead>
          <tr>
            {(
              [
                ["name", "Player"],
                ["joinDate", "Joined"],
                ["leaveDate", "Left"],
                ["alts", "Alts"],
              ] as const
            ).map(([key, label]) => (
              <th
                key={key}
                scope="col"
                aria-sort={sort.key === key ? sort.direction : undefined}
              >
                <SortButton
                  label={label}
                  direction={sort.key === key ? sort.direction : undefined}
                  onClick={() =>
                    setSort((current) => ({
                      key,
                      direction:
                        current.key === key && current.direction === "ascending"
                          ? "descending"
                          : "ascending",
                    }))
                  }
                />
              </th>
            ))}
            <th scope="col">
              <span className="sr-only">Details</span>
            </th>
          </tr>
        </thead>
        <tbody>
          {players.map((player) => (
            <Fragment key={player.id}>
              <tr className="interactive-row" onClick={() => toggle(player.id)}>
                <td>
                  <button
                    className="row-name roster-main-button"
                    aria-label={`Edit ${player.name}`}
                    aria-expanded={Boolean(
                      detail && detailPlayerId === player.id,
                    )}
                  >
                    <strong style={{ color: colors[player.id] }}>
                      {player.name}
                    </strong>
                    {!player.active && (
                      <span className="roster-inactive">Inactive</span>
                    )}
                  </button>
                </td>
                <td>{player.joinDate || "—"}</td>
                <td>{player.leaveDate || "—"}</td>
                <td onClick={(event) => event.stopPropagation()}>
                  <div className="roster-alts">
                    {visibleAlts.get(player.id)!.map((alt, index) => (
                      <Fragment key={alt.id}>
                        {index > 0 && ", "}
                        <button
                          className="roster-alt-button"
                          onClick={() => editAlt(alt.id)}
                          title={`${alt.realm || "All realms"} · ${alt.region.toUpperCase()}`}
                          aria-label={`Edit alt ${alt.name} for ${player.name}`}
                        >
                          {alt.name}
                          {characters.filter(
                            (other) =>
                              other.playerId === player.id &&
                              other.name.toLocaleLowerCase() ===
                                alt.name.toLocaleLowerCase(),
                          ).length > 1 && (
                            <small>
                              {" "}
                              · {alt.realm || "All realms"} ·{" "}
                              {alt.region.toUpperCase()}
                            </small>
                          )}
                        </button>
                      </Fragment>
                    ))}
                  </div>
                </td>
                <td>
                  <button
                    className="roster-row-toggle"
                    aria-label={`${detail && detailPlayerId === player.id ? "Collapse" : "Details for"} ${player.name}`}
                    aria-expanded={Boolean(
                      detail && detailPlayerId === player.id,
                    )}
                  >
                    {detail && detailPlayerId === player.id ? (
                      <ChevronDown size={14} />
                    ) : (
                      <ChevronRight size={14} />
                    )}
                  </button>
                </td>
              </tr>
              {detail && detailPlayerId === player.id && (
                <tr className="detail-row roster-detail-row">
                  <td colSpan={5} className="roster-detail-cell">
                    {detail}
                  </td>
                </tr>
              )}
            </Fragment>
          ))}
        </tbody>
      </DataTable>
      {characters.some(
        (alt) => !roster.some((player) => player.id === alt.playerId),
      ) && (
        <details className="roster-orphan-alts">
          <summary>Alts needing a main</summary>
          {characters
            .filter(
              (alt) => !roster.some((player) => player.id === alt.playerId),
            )
            .map((alt) => (
              <button
                key={alt.id}
                className="roster-alt-button"
                onClick={() => editAlt(alt.id)}
              >
                {alt.name}
              </button>
            ))}
        </details>
      )}
      {!players.length && (
        <p className="empty-state">
          {roster.length
            ? "No matching players or alts."
            : "No mains in the roster. Add the first main to get started."}
        </p>
      )}
    </section>
  );
}
