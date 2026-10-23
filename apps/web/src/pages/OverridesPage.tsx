import { Clock3, Search, Info, ChevronRight } from "lucide-react";
import {
  adjustmentKey,
  formatDate,
  type Inputs,
  type Player,
  type NightResult,
  type Timing,
} from "../model";
import { Notice, Badge } from "../app/Feedback";
import { SelectControl } from "../SelectControl";
import { RunStatus } from "../RunStatus";
import { OverrideTabs } from "../OverrideTabs";
import { PanelHeader } from "../PanelHeader";
import { SortButton } from "../SortButton";
import { AvailabilityEditor } from "../AvailabilityEditor";
import { TimingEditor } from "../TimingEditor";
import type { AvailabilityEditorState } from "../useAvailabilityEditor";
import type { TimingEditorState } from "../useTimingEditor";
export type OverrideSort = {
  key: "name" | "preBench" | "postBench" | "prePlayed" | "postPlayed";
  direction: "ascending" | "descending";
};
export function OverridesPage({
  nightSelection,
  status,
  tab: overrideTab,
  onTabChange,
  availability,
  timing,
}: {
  nightSelection: {
    value: string;
    options: string[];
    onChange: (night: string) => void;
  };
  status: { message: string; completedAt?: string; retryQueue?: () => void };
  tab: "availability" | "timing";
  onTabChange: (tab: "availability" | "timing") => void;
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
  timing: { detected: Timing; editor: TimingEditorState };
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
  const { detected: detectedTiming, editor: timingEditor } = timing;
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
          </div>
          <OverrideTabs value={overrideTab} onChange={onTabChange} />
          {overrideTab === "availability" ? (
            <div
              className="override-grid"
              id="override-panel"
              role="tabpanel"
              aria-labelledby="override-tab-availability"
            >
              <section className="card player-card">
                <PanelHeader
                  title="Who was available?"
                  description="Select a player to adjust their availability."
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
                          ],
                        ],
                        [
                          "Played",
                          [
                            ["prePlayed", "Before"],
                            ["postPlayed", "After"],
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
                              aria-label={`Sort by ${group === "Player" ? "Player" : `${group} ${label.toLowerCase()} break`}${overrideSort.key === key ? `, ${overrideSort.direction}` : ""}`}
                              onClick={() => changeOverrideSort(key)}
                            />
                          ))}
                        </span>
                      </span>
                    ))}
                  </div>
                  {visiblePlayers.map((player) => {
                    const a =
                      inputs.adjustments[adjustmentKey(night, player.id)];
                    const overridden =
                      a &&
                      (a.pre.mode !== "automatic" ||
                        a.post.mode !== "automatic");
                    const row = latestNight?.players.find(
                      (r) => r.id === player.id,
                    );
                    const preOverride =
                      (row?.availability?.pre.mode ??
                        a?.pre.mode ??
                        "automatic") !== "automatic";
                    const postOverride =
                      (row?.availability?.post.mode ??
                        a?.post.mode ??
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
                        {(overridden || row?.overridden) && (
                          <Badge tone="purple">Override</Badge>
                        )}
                        {(
                          [
                            ["Bench before break", row?.preBench, preOverride],
                            ["Bench after break", row?.postBench, postOverride],
                            [
                              "Played before break",
                              row?.prePlayed,
                              preOverride,
                            ],
                            [
                              "Played after break",
                              row?.postPlayed,
                              postOverride,
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
                  {!visiblePlayers.length && (
                    <p className="empty-state" role="status">
                      {players.length
                        ? "No matching players. Try another search."
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
              />
            </div>
          ) : (
            <div
              id="override-panel"
              role="tabpanel"
              aria-labelledby="override-tab-timing"
            >
              <TimingEditor
                night={night}
                detected={detectedTiming}
                editor={timingEditor}
              />
            </div>
          )}
        </>
      )}
    </>
  );
}
