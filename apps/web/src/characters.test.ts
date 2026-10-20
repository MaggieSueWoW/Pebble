import { describe, expect, it } from "vitest";
import {
  blankCharacter,
  characterIdentity,
  identityCasefold,
  normalizeCharacter,
  validateCharacter,
} from "./characters";
import { blankPlayer } from "./roster";
const roster = [
  { ...blankPlayer(), id: "main", version: 1, name: "Example", active: false },
];
const value = {
  ...blankCharacter(),
  name: "Étoile",
  realm: "Example Realm",
  playerId: "main",
};
describe("character identities", () => {
  it("trims surrounding whitespace, normalizes region and retains display casing", () => {
    expect(
      normalizeCharacter({
        ...value,
        name: " Étoile ",
        realm: " Example Realm ",
        region: " US ",
      }),
    ).toEqual(value);
  });
  it("uses Unicode casefold matching including expansions without conflating dotless i", () => {
    for (const [input, folded] of [
      ["Straße", "strasse"],
      ["ẞ", "ss"],
      ["ΟΣ", "οσ"],
      ["ος", "οσ"],
      ["ﬃ", "ffi"],
      ["Ꭰꭰ", "ᎠᎠ"],
      ["İıI", "i̇ıi"],
    ])
      expect(identityCasefold(input)).toBe(folded);
    expect(characterIdentity(value)).toBe(
      characterIdentity({ ...value, name: "éTOILE", realm: "EXAMPLE REALM" }),
    );
  });
  it("rejects duplicate identities but permits different regions and wildcard/exact realm pairs", () => {
    const characters = [{ ...value, id: "old", version: 1 }];
    expect(
      validateCharacter(
        { ...value, name: "éTOILE", realm: " example realm " },
        characters,
        roster,
        "new",
      ),
    ).toContain("already have a mapping");
    expect(validateCharacter(value, characters, roster, "old")).toBeNull();
    expect(
      validateCharacter({ ...value, region: "eu" }, characters, roster, "new"),
    ).toBeNull();
    expect(
      validateCharacter({ ...value, realm: "" }, characters, roster, "new"),
    ).toBeNull();
  });
  it("validates names, region, realm length and existing main including inactive choices", () => {
    for (const change of [
      { name: "" },
      { name: "Name-Realm" },
      { name: "x".repeat(33) },
      { realm: "x".repeat(101) },
      { region: "unknown" },
      { playerId: "missing" },
    ])
      expect(
        validateCharacter({ ...value, ...change }, [], roster, "new"),
      ).toBeTruthy();
    expect(validateCharacter(value, [], roster, "new")).toBeNull();
  });
});
