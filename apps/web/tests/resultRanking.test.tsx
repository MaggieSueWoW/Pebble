// @vitest-environment jsdom
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { orderResultRows, sortResultRows } from "../src/resultRanking";
import { ResultRow } from "../src/ResultRow";
import type { PlayerResult } from "../src/model";

const row = (id: string, bench: number, played = 100): PlayerResult => ({
  id,
  name: id,
  bench,
  played,
  preBench: bench,
  postBench: 0,
  overridden: false,
  nights: 1,
});
afterEach(cleanup);

it("uses recorded season ranks/exclusions and retains zero-time ranked mains", () => {
  const input: PlayerResult[] = [
    {
      ...row("alpha", 20),
      ranking: { rank: 3, played: 100, bench: 20, ratio: 0.2 },
    },
    { ...row("Former", 1), ranking: null },
    {
      ...row("Zulu", 20),
      ranking: { rank: 2, played: 100, bench: 20, ratio: 0.2 },
    },
    {
      ...row("Zero", 0, 0),
      ranking: { rank: 1, played: 0, bench: 0, ratio: null },
    },
  ];
  const output = orderResultRows(
    input,
    "season",
    () => "Changed live roster label",
  );
  expect(output.map((p) => p.id)).toEqual(["Zero", "Zulu", "alpha"]);
  expect(output.map((p) => p.ranking?.rank)).toEqual([1, 2, 3]);
  expect(input.map((p) => p.id)).toEqual(["alpha", "Former", "Zulu", "Zero"]);
});

it("keeps historical night/week eligibility independent of current season rankings", () => {
  const historical = { ...row("Former", 1), ranking: null };
  const zero = row("Zero", 0, 0);
  expect(orderResultRows([historical, zero], "night").map((p) => p.id)).toEqual(
    ["Former"],
  );
  expect(orderResultRows([historical, zero], "week").map((p) => p.id)).toEqual([
    "Zero",
    "Former",
  ]);
});

it("orders tied recorded bench values by binary names rather than locale or ratio", () => {
  const input = ["𐐀", "alpha", "Ａ", "Zulu", "Éclair"].map((name) =>
    row(name, 20),
  );
  expect(orderResultRows(input, "week").map((p) => p.id)).toEqual([
    "Zulu",
    "alpha",
    "Éclair",
    "Ａ",
    "𐐀",
  ]);
  expect(
    orderResultRows([row("High", 21), row("Low", 1)], "season").map(
      (p) => p.id,
    ),
  ).toEqual(["Low", "High"]);
});

it("preserves recorded rank for a zero-time row without displaying a ratio", () => {
  render(
    <table>
      <tbody>
        <ResultRow
          player={{
            id: "example",
            name: "Example",
            className: "Mage",
            role: "Damage",
            color: "#b9c7d8",
            pre: 0,
            post: 0,
          }}
          row={row("example", 0, 0)}
          index={0}
          rank={3}
          expanded
          onExpand={vi.fn()}
        />
      </tbody>
    </table>,
  );
  expect(screen.getByText("03")).toBeTruthy();
  expect(screen.queryByText("Bench:played ratio")).toBeNull();
});

it("sorts displayed names and numeric times independently of recorded rank", () => {
  const input = [
    row("Beta", 100, 20),
    row("Alpha", 20, 100),
    row("Gamma", 3, 80),
  ];
  const names = (
    key: "name" | "bench" | "played",
    direction: "ascending" | "descending",
  ) => sortResultRows(input, { key, direction }).map((row) => row.id);
  expect(names("name", "ascending")).toEqual(["Alpha", "Beta", "Gamma"]);
  expect(names("name", "descending")).toEqual(["Gamma", "Beta", "Alpha"]);
  expect(names("bench", "ascending")).toEqual(["Gamma", "Alpha", "Beta"]);
  expect(names("bench", "descending")).toEqual(["Beta", "Alpha", "Gamma"]);
  expect(names("played", "descending")).toEqual(["Alpha", "Gamma", "Beta"]);
  expect(input.map((row) => row.id)).toEqual(["Beta", "Alpha", "Gamma"]);
  expect(sortResultRows(input, null)).toBe(input);
});
it("sorts Time Played by actual share, keeps undefined shares last, and breaks ties by name", () => {
  const input = [
    row("Zero", 0, 0),
    row("Beta", 20, 80),
    row("Alpha", 2, 8),
    row("High", 1, 99),
  ];
  expect(
    sortResultRows(input, { key: "share", direction: "descending" }).map(
      (row) => row.id,
    ),
  ).toEqual(["High", "Alpha", "Beta", "Zero"]);
  expect(
    sortResultRows(input, { key: "share", direction: "ascending" }).map(
      (row) => row.id,
    ),
  ).toEqual(["Alpha", "Beta", "High", "Zero"]);
});
