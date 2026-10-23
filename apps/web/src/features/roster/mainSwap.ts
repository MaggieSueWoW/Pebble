import {
  identityCasefold,
  validateCharacter,
  type CharacterEntry,
  type CharacterValue,
} from "./characters";
import { validatePlayer, type RosterEntry, type RosterValue } from "./roster";

/** Keep the player's stable identity while exchanging its main and alt names. */
export function planMainSwap(
  main: RosterEntry,
  alt: CharacterEntry,
  roster: RosterEntry[],
  characters: CharacterEntry[],
) {
  if (alt.playerId !== main.id)
    throw new Error("This alt no longer belongs to this main.");
  if (identityCasefold(main.name) === identityCasefold(alt.name))
    throw new Error("This character already has the main's name.");
  const player: RosterValue = {
    name: alt.name,
    className: main.className,
    role: main.role,
    active: main.active,
    joinDate: main.joinDate,
    leaveDate: main.leaveDate,
    notes: main.notes,
  };
  const playerError = validatePlayer(player, roster, main.id);
  if (playerError) throw new Error(playerError);
  const replacement: CharacterValue = {
    name: main.name,
    realm: "",
    region: alt.region,
    playerId: main.id,
  };
  const reused = characters.find(
    (entry) =>
      entry.id !== alt.id &&
      identityCasefold(entry.name) === identityCasefold(main.name) &&
      entry.region.toLowerCase() === alt.region.toLowerCase() &&
      entry.playerId === main.id,
  );
  if (!reused) {
    const error = validateCharacter(replacement, characters, roster, alt.id);
    if (error) throw new Error(error);
  }
  return { player, replacement, reused };
}
