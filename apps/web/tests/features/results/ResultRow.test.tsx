// @vitest-environment jsdom
import { cleanup, render, screen, within } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { ResultRow } from "../../../src/features/results/ResultRow";
import type {
  Player,
  PlayerResult,
  WeeklyPlayerResult,
} from "../../../src/shared/model";

const player: Player = {
  id: "example",
  name: "Example Player",
  className: "Mage",
  role: "Damage",
  color: "#b9c7d8",
  pre: 0,
  post: 0,
};
const base: PlayerResult = {
  id: player.id,
  name: player.name,
  played: 90,
  bench: 30,
  preBench: 10,
  postBench: 20,
  overridden: false,
  nights: 1,
};
function show(row: PlayerResult | WeeklyPlayerResult, onEdit?: () => void) {
  render(
    <table>
      <tbody>
        <ResultRow
          player={player}
          row={row}
          index={0}
          expanded
          onExpand={vi.fn()}
          onEdit={onEdit}
        />
      </tbody>
    </table>,
  );
  return screen.getByRole("region", {
    name: "Availability breakdown for Example Player",
  });
}
afterEach(cleanup);

it("displays recorded pre/post played and bench values including zero without inference", () => {
  const breakdown = show({ ...base, prePlayed: 0, postPlayed: 90 });
  expect(
    document.querySelector(".result-player-row .bench-value")?.textContent,
  ).toBe("30m");
  expect(
    document.querySelectorAll(".result-player-row .numeric")[1]?.textContent,
  ).toBe("1h 30m");
  const rows = within(breakdown).getAllByRole("row");
  expect(rows.map((row) => row.textContent)).toEqual([
    "PeriodBenchPlayedReason",
    "Before break10m-Not recorded",
    "After break20m1h 30mNot recorded",
  ]);
  expect(breakdown.querySelectorAll("details")).toHaveLength(0);
  expect(screen.queryByText("Adjusted")).toBeNull();
});

it("keeps missing recorded halves unavailable rather than splitting totals", () => {
  const breakdown = show({ ...base, prePlayed: 12, overridden: true }, vi.fn());
  const rows = within(breakdown).getAllByRole("row");
  expect(rows[1].textContent).toBe("Before break10m12mNot recorded");
  expect(rows[2].textContent).toBe("After break20mNot recordedNot recorded");
  expect(breakdown.textContent).not.toContain("78 min");
  expect(screen.getByText("Adjusted")).toBeTruthy();
  expect(
    within(breakdown).getByRole("button", { name: "Edit availability" }),
  ).toBeTruthy();
});

it("marks both played halves missing on older result documents", () => {
  const breakdown = show(base);
  expect(within(breakdown).getAllByText("Not recorded")).toHaveLength(4);
  expect(breakdown.textContent).not.toContain("undefined");
  expect(breakdown.textContent).not.toContain("NaN");
});

it("omits unavailable weekly played halves and adjustment provenance", () => {
  const breakdown = show({
    id: player.id,
    played: 123,
    bench: 47,
    preBench: 20,
    postBench: 27,
  });
  expect(breakdown.textContent).not.toContain("break played");
  expect(breakdown.textContent).not.toContain("Not recorded");
  expect(breakdown.textContent).not.toContain("Automatically inferred");
  expect(breakdown.textContent).not.toContain("Officer adjustment applied");
  expect(within(breakdown).getAllByRole("row")[1].textContent).toBe(
    "Before break20m",
  );
});

it("shows attendance only when requested for a season row, preserving nightly and weekly scope", () => {
  const row = {
    ...base,
    overridden: true,
    attendance: { played: 60, bench: 30, possible: 120, rate: 0.75, weeks: [] },
  };
  const { rerender } = render(
    <table>
      <tbody>
        <ResultRow
          player={player}
          row={row}
          index={0}
          expanded
          onExpand={vi.fn()}
          showAttendance
        />
      </tbody>
    </table>,
  );
  expect(screen.getByRole("region", { name: "Attendance" })).toBeTruthy();
  expect(screen.queryByText("Adjusted")).toBeNull();
  expect(document.querySelectorAll("details")).toHaveLength(0);
  expect(screen.queryByRole("columnheader", { name: "Reason" })).toBeNull();
  expect(
    within(screen.getByRole("region", { name: "Attendance" })).getByText(
      "75%",
      { exact: true },
    ),
  ).toBeTruthy();
  rerender(
    <table>
      <tbody>
        <ResultRow
          player={player}
          row={row}
          index={0}
          expanded
          onExpand={vi.fn()}
        />
      </tbody>
    </table>,
  );
  expect(screen.queryByRole("region", { name: "Attendance" })).toBeNull();
  expect(screen.getByRole("rowheader", { name: "Before break" })).toBeTruthy();
});

it("rounds displayed durations without changing the underlying played share", () => {
  show({
    ...base,
    bench: 29.6,
    played: 90.4,
    preBench: 9.6,
    postBench: 20,
    prePlayed: 12.4,
    postPlayed: 78,
  });
  expect(document.querySelector(".bench-value")?.textContent).toBe("30m");
  expect(
    document.querySelectorAll(".result-player-row .numeric")[1]?.textContent,
  ).toBe("1h 30m");
  expect(
    screen.getByLabelText("90 minutes played, 30 minutes benched"),
  ).toBeTruthy();
  expect(screen.getByText("75%")).toBeTruthy();
  expect(
    screen.getByRole("rowheader", { name: "Before break" }).parentElement
      ?.textContent,
  ).toBe("Before break10m12mNot recorded");
});

it("marks only the adjusted period and puts its reason directly in the table", () => {
  const breakdown = show({
    ...base,
    prePlayed: 20,
    postPlayed: 70,
    overridden: true,
    availability: {
      pre: { available: true, mode: "partial" },
      post: { available: true, mode: "automatic" },
      source: "override",
    },
  });
  const rows = within(breakdown).getAllByRole("row");
  expect(rows[1].querySelectorAll(".result-adjusted-time")).toHaveLength(2);
  expect(breakdown.textContent).not.toContain("*");
  expect(rows[1].textContent).toContain("Available for part of the period");
  expect(rows[2].querySelectorAll(".result-adjusted-time")).toHaveLength(0);
  expect(rows[2].textContent).toContain("Available · Detected");
  expect(breakdown.querySelectorAll("details")).toHaveLength(0);
  expect(breakdown.textContent).not.toContain("Overall source");
});
