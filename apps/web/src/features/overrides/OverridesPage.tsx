import { useState, type ReactNode } from "react";
import { Clock3, Search, Info, ChevronRight } from "lucide-react";
import {
  adjustmentKey,
  formatDate,
  type Inputs,
  type Player,
  type NightResult,
} from "../../shared/model";
import { Notice, Badge } from "../../components/Feedback";
import { SelectControl } from "../../components/SelectControl";
import { RunStatus } from "../../components/RunStatus";
import { PanelHeader } from "../../components/PanelHeader";
import { SortButton } from "../../components/SortButton";
import { AvailabilityEditor } from "./AvailabilityEditor";
import type { AvailabilityEditorState } from "./useAvailabilityEditor";
export type OverrideSort = {
  key: "name" | "preBench" | "postBench" | "bench";
  direction: "ascending" | "descending";
};
export function OverridesPage({
  nightSelection,
  status,
  availability,
  nextAction,
}: {
  nextAction?: ReactNode;
  nightSelection: {
    value: string;
    options: string[];
    onChange: (night: string) => void;
  };
  status: { message: string; completedAt?: string; retryQueue?: () => void };
  availability: {
    players: Player[];
    visiblePlayers: Player[];
    selectedPlayer?: Player;
    selected: string;
    search: string;
    setSearch: (search: string) => void;
    choosePlayer: (id: string) => void;
    sort: OverrideSort;
    onSort: (key: OverrideSort["key"]) => void;
    inputs: Inputs;
    latestNight?: NightResult;
    editor: AvailabilityEditorState;
    span: { pre: number; post: number };
  };
}) {
  const {
    value: night,
    options: nightOptions,
    onChange: chooseNight,
  } = nightSelection;
  const { message: runStatusMessage, completedAt, retryQueue } = status;
  const {
    players,
    visiblePlayers,
    selectedPlayer,
    selected,
    search,
    setSearch,
    choosePlayer,
    sort: overrideSort,
    onSort: changeOverrideSort,
    inputs,
    latestNight,
    editor: availabilityEditor,
    span,
  } = availability;
  const [overridesOnly, setOverridesOnly] = useState(false);
  function isOverridden(id: string) {
    const adjustment = inputs.adjustments[adjustmentKey(night, id)];
    if (adjustment)
      return (
        adjustment.pre.mode !== "automatic" ||
        adjustment.post.mode !== "automatic"
      );
    return (
      latestNight?.players.find((row) => row.id === id)?.overridden ?? false
    );
  }
  const adjustedCount = players.filter((player) =>
    isOverridden(player.id),
  ).length;
  const shownPlayers = overridesOnly
    ? visiblePlayers.filter((player) => isOverridden(player.id))
    : visiblePlayers;
  return (
    <>
      {!nightOptions.length && (
        <Notice>
          No raid nights identified yet. Add reports and run Pebble to identify
          nights before adding overrides.
        </Notice>
      )}
      {nightOptions.length > 0 && (
        <>
          <div className="override-toolbar toolbar">
            <label className="night-select">
              <Clock3 size={16} />
              <span>Raid night</span>
              <SelectControl
                aria-label="Raid night"
                value={night}
                onChange={(e) => chooseNight(e.target.value)}
              >
                {nightOptions.map((n) => (
                  <option key={n} value={n}>
                    {formatDate(n, true)}
                  </option>
                ))}
              </SelectControl>
            </label>
            <RunStatus message={runStatusMessage} completedAt={completedAt} />
            {retryQueue && (
              <button className="text-button" onClick={retryQueue}>
                Retry run queue
              </button>
            )}
            <div className="override-next">{nextAction}</div>
          </div>
          <div className="override-grid" id="override-panel">
            <section className="card player-card">
              <PanelHeader
                title="Who was available?"
                actions={
                  <Badge>
                    {adjustedCount} {adjustedCount === 1 ? "player" : "players"}{" "}
                    adjusted
                  </Badge>
                }
              />
              <label className="search-field">
                <Search size={16} />
                <input
                  aria-label="Search players"
                  placeholder="Find a player or class…"
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                />
              </label>
              <label className="override-filter">
                <input
                  type="checkbox"
                  checked={overridesOnly}
                  onChange={(event) => setOverridesOnly(event.target.checked)}
                />
                Overrides only
              </label>
              <div className="player-list">
                <div className="override-player-columns">
                  {(
                    [
                      ["Player", [["name", "Player"]]],
                      [
                        "Bench",
                        [
                          ["preBench", "Before"],
                          ["postBench", "After"],
                          ["bench", "Total"],
                        ],
                      ],
                    ] as const
                  ).map(([group, columns]) => (
                    <span key={group}>
                      {group !== "Player" && group}
                      <span
                        className={
                          group === "Player"
                            ? undefined
                            : "override-sort-halves"
                        }
                      >
                        {columns.map(([key, label]) => (
                          <SortButton
                            key={key}
                            label={label}
                            direction={
                              overrideSort.key === key
                                ? overrideSort.direction
                                : undefined
                            }
                            aria-pressed={overrideSort.key === key}
                            aria-label={`Sort by ${group === "Player" ? "Player" : key === "bench" ? "Total bench" : `${group} ${label.toLowerCase()} break`}${overrideSort.key === key ? `, ${overrideSort.direction}` : ""}`}
                            onClick={() => changeOverrideSort(key)}
                          />
                        ))}
                      </span>
                    </span>
                  ))}
                </div>
                {shownPlayers.map((player) => {
                  const a = inputs.adjustments[adjustmentKey(night, player.id)];
                  const overridden = isOverridden(player.id);
                  const row = latestNight?.players.find(
                    (r) => r.id === player.id,
                  );
                  const preOverride =
                    (a?.pre.mode ??
                      row?.availability?.pre.mode ??
                      "automatic") !== "automatic";
                  const postOverride =
                    (a?.post.mode ??
                      row?.availability?.post.mode ??
                      "automatic") !== "automatic";
                  return (
                    <button
                      key={player.id}
                      className={`player-item override-player-row ${selected === player.id ? "selected" : ""}`}
                      onClick={() => choosePlayer(player.id)}
                      aria-pressed={selected === player.id}
                    >
                      <span className="player-description">
                        <strong style={{ color: player.color }}>
                          {player.name}
                        </strong>
                      </span>
                      {overridden && <Badge tone="purple">Override</Badge>}
                      {(
                        [
                          ["Bench before break", row?.preBench, preOverride],
                          ["Bench after break", row?.postBench, postOverride],
                          [
                            "Total bench",
                            row?.bench,
                            preOverride || postOverride,
                          ],
                        ] as const
                      ).map(([label, minutes, affected], index) => (
                        <span
                          key={label}
                          style={{ gridColumn: index + 3 }}
                          className={`override-minute ${affected ? "override-affected" : ""}`}
                          aria-label={`${label}: ${minutes == null ? "not recorded" : `${minutes} minutes`}${affected ? ", availability override" : ""}`}
                          title={`${label}${affected ? " · Availability override" : ""}`}
                        >
                          {minutes == null
                            ? "—"
                            : minutes === 0
                              ? "-"
                              : minutes}
                        </span>
                      ))}
                      <ChevronRight size={14} />
                    </button>
                  );
                })}
                {!shownPlayers.length && (
                  <p className="empty-state" role="status">
                    {players.length
                      ? overridesOnly
                        ? "No matching availability overrides."
                        : "No matching players. Try another search."
                      : "No players in the roster yet."}
                  </p>
                )}
              </div>
              <div className="table-footnote">
                <Info size={14} /> Minutes from the last completed run.
                Highlighted values have an availability override.
              </div>
            </section>
            <AvailabilityEditor
              player={selectedPlayer}
              editor={availabilityEditor}
              span={span}
              recorded={latestNight?.players.find((row) => row.id === selected)}
            />
          </div>
        </>
      )}
    </>
  );
}
