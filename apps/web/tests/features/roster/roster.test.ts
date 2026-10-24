import { describe, it, expect } from "vitest";
import {
  blankPlayer,
  validatePlayer,
} from "../../../src/features/roster/roster";
describe("roster validation", () => {
  it("accepts Unicode main names and blank or valid membership dates", () => {
    expect(
      validatePlayer(
        {
          ...blankPlayer(),
          name: "Étoile",
          joinDate: "2026-10-01",
          leaveDate: "2026-10-01",
        },
        [],
        "new",
      ),
    ).toBeNull();
  });
  it("rejects names/duplicates, impossible dates, reversed dates and unsupported metadata", () => {
    const good = { ...blankPlayer(), name: "Example" };
    for (const change of [
      { name: "" },
      { name: "Player-Realm" },
      { name: "x".repeat(33) },
      { joinDate: "2026-02-30" },
      { joinDate: "0000-01-01" },
      { joinDate: "2026-10-02", leaveDate: "2026-10-01" },
      { className: "Wizard" },
      { role: "Officer" },
      { notes: "x".repeat(1001) },
    ])
      expect(validatePlayer({ ...good, ...change }, [], "new")).toBeTruthy();
    expect(
      validatePlayer(
        good,
        [{ ...good, name: "example", id: "other", version: 1 }],
        "new",
      ),
    ).toContain("already used");
    expect(
      validatePlayer(good, [{ ...good, id: "same", version: 1 }], "same"),
    ).toBeNull();
  });
});
