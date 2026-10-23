import { expect, it } from "vitest";
import { readWeekResults } from "../src/weeklyResults";

it("preserves stored weekly minutes, zero rows and historical names without private fields", () => {
  const row = {
    main: "Recorded Player",
    played_min: 123,
    bench_min: 47,
    bench_pre_min: 20,
    bench_post_min: 27,
    notes: "not public",
  };
  const weeks = readWeekResults(
    [
      { week: "2026-10-06", players: [row] },
      {
        week: "2026-10-13",
        players: [
          {
            ...row,
            main: "Historical Player",
            played_min: 0,
            bench_min: 0,
            bench_pre_min: 0,
            bench_post_min: 0,
          },
          row,
        ],
      },
    ],
    [
      {
        id: "recorded-id",
        name: row.main,
        className: "Mage",
        role: "Damage",
        played: 999,
        prePlayed: 500,
        postPlayed: 499,
        bench: 999,
        preBench: 999,
        postBench: 0,
        nights: 4,
        overridden: true,
      },
    ],
  );
  expect(weeks.map((w) => w.week)).toEqual(["2026-10-13", "2026-10-06"]);
  expect(weeks[0].players[1]).toEqual({
    id: "recorded-id",
    name: row.main,
    className: "Mage",
    role: "Damage",
    color: undefined,
    played: 123,
    bench: 47,
    preBench: 20,
    postBench: 27,
  });
  expect(weeks[0].players[0]).toMatchObject({
    id: "Historical Player",
    name: "Historical Player",
    played: 0,
    bench: 0,
  });
  expect(JSON.stringify(weeks)).not.toContain("not public");
  expect(weeks[0].players[1]).not.toHaveProperty("overridden");
  expect(weeks[0].players[1]).not.toHaveProperty("prePlayed");
  expect(weeks[0].players[1]).not.toHaveProperty("postPlayed");
});
