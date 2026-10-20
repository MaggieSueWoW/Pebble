import { useEffect, useState } from "react";
import {
  blankCharacter,
  validateCharacter,
  type CharacterEntry,
  type CharacterValue,
} from "./characters";
import type { RosterEntry } from "./roster";
import { useVersionedEditor } from "./useVersionedEditor";
export function useCharacterEditor(
  characters: CharacterEntry[],
  roster: RosterEntry[],
  saveValue: (
    id: string,
    value: CharacterValue,
    expected: number,
  ) => Promise<number>,
  busy: boolean,
  enabled: boolean,
) {
  const [chosen, setChosen] = useState<{
    id: string;
    creating: boolean;
  } | null>(() =>
    characters[0] ? { id: characters[0].id, creating: false } : null,
  );
  const id = chosen?.id ?? characters[0]?.id ?? "";
  const character = characters.find((entry) => entry.id === id);
  const {
    id: _id,
    version: _version,
    ...saved
  } = character ?? { id, version: 0, ...blankCharacter() };
  const editor = useVersionedEditor({
    key: id,
    saved,
    version: character?.version ?? 0,
    validate: (value) => validateCharacter(value, characters, roster, id),
    saveValue,
    resetValue: blankCharacter,
    busy,
    enabled: enabled && Boolean(id),
    label: "Character mapping",
  });
  useEffect(() => {
    if (character && chosen?.creating)
      setChosen({ id: character.id, creating: false });
    else if (!chosen && characters[0])
      setChosen({ id: characters[0].id, creating: false });
    else if (
      chosen &&
      !chosen.creating &&
      !character &&
      !editor.dirty &&
      !editor.saving
    )
      setChosen(
        characters[0] ? { id: characters[0].id, creating: false } : null,
      );
  }, [chosen, character, characters, editor.dirty, editor.saving]);
  return {
    ...editor,
    id,
    exists: Boolean(character),
    select: (id: string) => setChosen({ id, creating: false }),
    add: () => setChosen({ id: crypto.randomUUID(), creating: true }),
  };
}
export type CharacterEditorState = ReturnType<typeof useCharacterEditor>;
