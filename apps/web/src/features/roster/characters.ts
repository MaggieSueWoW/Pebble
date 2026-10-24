import type { RosterEntry } from "./roster";
export const regions = ["us", "eu", "kr", "tw", "cn"] as const;
export type CharacterValue = {
  name: string;
  realm: string;
  region: string;
  playerId: string;
};
export type CharacterEntry = CharacterValue & {
  id: string;
  version: number;
  ignored?: boolean;
};
export const blankCharacter = (): CharacterValue => ({
  name: "",
  realm: "",
  region: "us",
  playerId: "",
});
/** Fold identities like the worker, using runtime Unicode case data.
 * Worker domain validation remains authoritative across runtime Unicode versions.
 */
export function identityCasefold(value: string): string {
  return Array.from(value, (char) => {
    if (char === "ı") return char;
    if (char === "ẞ") return "ss";
    const folded = char.toUpperCase().toLowerCase();
    // Unicode casefold keeps Cherokee in uppercase, unlike ordinary lowercasing.
    return Array.from(folded, (part) =>
      /[\u13a0-\u13ff\uab70-\uabbf]/u.test(part) ? part.toUpperCase() : part,
    ).join("");
  }).join("");
}
export function normalizeCharacter(value: CharacterValue): CharacterValue {
  return {
    ...value,
    name: value.name.trim(),
    realm: value.realm.trim(),
    region: value.region.trim().toLowerCase(),
  };
}
export function characterIdentity(value: CharacterValue): string {
  const normalized = normalizeCharacter(value);
  return JSON.stringify([
    normalized.region,
    identityCasefold(normalized.name),
    identityCasefold(normalized.realm),
  ]);
}
export function validateCharacter(
  raw: CharacterValue,
  characters: CharacterEntry[],
  roster: RosterEntry[],
  id: string,
): string | null {
  const value = normalizeCharacter(raw);
  if (!/^\p{L}{1,32}$/u.test(value.name))
    return "Character name must contain 1–32 letters only, without a realm suffix.";
  if (Array.from(value.realm).length > 100)
    return "Realm must be 100 characters or fewer.";
  if (!regions.includes(value.region as (typeof regions)[number]))
    return "Choose a listed region.";
  if (!roster.some((player) => player.id === value.playerId))
    return "Choose an existing main player.";
  if (
    characters.some(
      (character) =>
        character.id !== id &&
        !character.ignored &&
        characterIdentity(character) === characterIdentity(value),
    )
  )
    return "This character, realm and region already have a mapping. Edit that mapping instead.";
  return null;
}
