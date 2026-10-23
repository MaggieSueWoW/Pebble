import { useEffect, useState } from "react";
import {
  blankPlayer,
  validatePlayer,
  type RosterEntry,
  type RosterValue,
} from "./roster";
import { useVersionedEditor } from "../../shared/useVersionedEditor";
export function useRosterEditor(
  roster: RosterEntry[],
  saveValue: (
    id: string,
    value: RosterValue,
    expected: number,
  ) => Promise<number>,
  busy: boolean,
  enabled: boolean,
) {
  const [chosen, setChosen] = useState<{
    id: string;
    creating: boolean;
  } | null>(() => (roster[0] ? { id: roster[0].id, creating: false } : null));
  const id = chosen?.id ?? roster[0]?.id ?? "";
  const player = roster.find((p) => p.id === id);
  const {
    id: _id,
    version: _version,
    ...saved
  } = player ?? { id, version: 0, ...blankPlayer() };
  const editor = useVersionedEditor({
    key: id,
    saved,
    version: player?.version ?? 0,
    validate: (value) => validatePlayer(value, roster, id),
    saveValue,
    resetValue: blankPlayer,
    busy,
    enabled: enabled && Boolean(id),
    label: "Roster",
  });
  useEffect(() => {
    if (player && chosen?.creating)
      setChosen({ id: player.id, creating: false });
    else if (!chosen && roster[0])
      setChosen({ id: roster[0].id, creating: false });
    else if (
      chosen &&
      !chosen.creating &&
      !player &&
      !editor.dirty &&
      !editor.saving
    )
      setChosen(roster[0] ? { id: roster[0].id, creating: false } : null);
  }, [chosen, player, roster, editor.dirty, editor.saving]);
  return {
    ...editor,
    id,
    exists: Boolean(player),
    select: (id: string) => setChosen({ id, creating: false }),
    add: () => setChosen({ id: crypto.randomUUID(), creating: true }),
  };
}
export type RosterEditorState = ReturnType<typeof useRosterEditor>;
