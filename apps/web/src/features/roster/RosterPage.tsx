import type { Dispatch, SetStateAction } from "react";
import type { Inputs, Player } from "../../shared/model";
import type { LiveConnection } from "../../data/useFirestore";
import type { RosterEditorState } from "./useRosterEditor";
import type { CharacterEditorState } from "./useCharacterEditor";
import { RosterList } from "./RosterList";
import { RosterPanel } from "./RosterPanel";
import { CharacterMappingsPanel } from "./CharacterMappingsPanel";
export function RosterPage({
  connection,
  players,
  inputs,
  guard,
  rosterEditor,
  characterEditor,
  rosterDetail,
  setRosterDetail,
}: {
  connection: LiveConnection;
  players: Player[];
  inputs: Inputs;
  guard: (action: () => void) => void;
  rosterEditor: RosterEditorState;
  characterEditor: CharacterEditorState;
  rosterDetail: "main" | "alt" | null;
  setRosterDetail: Dispatch<SetStateAction<"main" | "alt" | null>>;
}) {
  return (
    <RosterList
      close={() => guard(() => setRosterDetail(null))}
      roster={connection.roster}
      characters={connection.characters}
      colors={Object.fromEntries(
        players.map((player) => [player.id, player.color]),
      )}
      editMain={(id) => {
        if (rosterDetail !== "main" || rosterEditor.id !== id)
          guard(() => {
            rosterEditor.select(id);
            setRosterDetail("main");
          });
      }}
      addMain={() =>
        guard(() => {
          rosterEditor.add();
          setRosterDetail("main");
        })
      }
      editAlt={(id) => {
        if (rosterDetail !== "alt" || characterEditor.id !== id)
          guard(() => {
            const alt = connection.characters.find((alt) => alt.id === id);
            if (alt) rosterEditor.select(alt.playerId);
            characterEditor.select(id);
            setRosterDetail("alt");
          });
      }}
      detailPlayerId={
        rosterDetail && rosterEditor.exists ? rosterEditor.id : undefined
      }
      detail={
        rosterDetail ? (
          <div className="merged-roster-details">
            <RosterPanel
              editor={rosterEditor}
              cancelCreate={() => setRosterDetail(null)}
              deletionBlocked={characterEditor.dirty || characterEditor.saving}
              linkedAlts={
                connection.characters.filter(
                  (alt) => alt.playerId === rosterEditor.id,
                ).length
              }
              linkedOverrides={
                Object.keys(inputs.adjustments).filter(
                  (key) => key.slice(key.indexOf("/") + 1) === rosterEditor.id,
                ).length
              }
              remove={async () => {
                const expected = Object.fromEntries(
                  Object.entries(connection.versions).filter(
                    ([path]) =>
                      path === `players/${rosterEditor.id}` ||
                      (path.startsWith("characters/") &&
                        connection.characters.some(
                          (alt) =>
                            alt.id === path.slice(11) &&
                            alt.playerId === rosterEditor.id,
                        )) ||
                      (path.startsWith("availabilityOverrides/") &&
                        path.slice("availabilityOverrides/".length + 11) ===
                          rosterEditor.id),
                  ),
                );
                await connection.deletePlayer(rosterEditor.id, expected);
                setRosterDetail(null);
              }}
            />
            {rosterEditor.exists && (
              <CharacterMappingsPanel
                characters={connection.characters}
                mainName={
                  connection.roster.find(
                    (player) => player.id === rosterEditor.id,
                  )?.name ?? rosterEditor.value.name
                }
                mainClass={rosterEditor.value.className}
                actionsBlocked={rosterEditor.dirty || rosterEditor.saving}
                swap={async (id, version) => {
                  const main = connection.roster.find(
                    (player) => player.id === rosterEditor.id,
                  );
                  if (!main)
                    throw new Error("This main is no longer in the roster.");
                  await connection.swapMain(main.id, id, main.version, version);
                  setRosterDetail("main");
                }}
                playerId={rosterEditor.id}
                editor={characterEditor}
                editing={rosterDetail === "alt"}
                cancelAdd={() => setRosterDetail("main")}
                edit={(id) => {
                  if (!characterEditor.dirty && !characterEditor.saving) {
                    characterEditor.select(id);
                    setRosterDetail("alt");
                  } else
                    guard(() => {
                      characterEditor.select(id);
                      setRosterDetail("alt");
                    });
                }}
                add={() => {
                  const action = () => {
                    characterEditor.add(rosterEditor.id);
                    setRosterDetail("alt");
                  };
                  if (!characterEditor.dirty && !characterEditor.saving)
                    action();
                  else guard(action);
                }}
                remove={async (id, version) => {
                  await connection.deleteCharacter(id, version);
                  if (characterEditor.id === id) setRosterDetail("main");
                }}
              />
            )}
          </div>
        ) : null
      }
    />
  );
}
