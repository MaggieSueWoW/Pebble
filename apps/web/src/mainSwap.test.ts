import { expect, it } from "vitest";
import { planMainSwap } from "./mainSwap";
import { blankPlayer } from "./roster";
const main = {
  ...blankPlayer(),
  id: "main",
  name: "Original",
  className: "Mage",
  notes: "Private",
  joinDate: "2026-10-01",
  version: 4,
};
const alt = {
  id: "alt",
  name: "Nextmain",
  realm: "Example realm",
  region: "eu",
  playerId: "main",
  version: 2,
};
it("preserves membership and creates an old-main alias without reusing the promoted character's realm", () => {
  const plan = planMainSwap(main, alt, [main], [alt]);
  expect(plan.player).toEqual({
    ...blankPlayer(),
    name: "Nextmain",
    className: "Mage",
    notes: "Private",
    joinDate: "2026-10-01",
  });
  expect(plan.replacement).toEqual({
    name: "Original",
    realm: "",
    region: "eu",
    playerId: "main",
  });
  expect(plan.reused).toBeUndefined();
  expect(alt.realm).toBe("Example realm");
});
it("reuses an existing old-main alias for the same player", () => {
  const original = { ...alt, id: "old", name: "original", realm: "" };
  expect(planMainSwap(main, alt, [main], [alt, original]).reused).toEqual(
    original,
  );
});
it("rejects another main's name or character mapping", () => {
  expect(() =>
    planMainSwap(
      main,
      alt,
      [main, { ...main, id: "other", name: "Nextmain" }],
      [alt],
    ),
  ).toThrow("already used");
  expect(() =>
    planMainSwap(
      main,
      alt,
      [main],
      [
        alt,
        { ...alt, id: "other", name: "Original", realm: "", playerId: "other" },
      ],
    ),
  ).toThrow("already have a mapping");
});
it("rejects alts from another player and a character that already has the main's name", () => {
  expect(() =>
    planMainSwap(main, { ...alt, playerId: "other" }, [main], [alt]),
  ).toThrow("no longer belongs");
  expect(() =>
    planMainSwap(main, { ...alt, name: "ORIGINAL" }, [main], [alt]),
  ).toThrow("already has");
});

it("reuses the promoted main's recorded realm mapping when swapping back", () => {
  const promoted = { ...main, name: "Nextmain" };
  const original = { ...alt, id: "old-main", name: "Original", realm: "" };
  const plan = planMainSwap(promoted, original, [promoted], [alt, original]);
  expect(plan.reused).toEqual(alt);
  expect(plan.player.name).toBe("Original");
});
