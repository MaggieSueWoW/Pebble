// @vitest-environment jsdom
import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { ResultRow } from "../src/ResultRow";
import type { Player, PlayerResult, WeeklyPlayerResult } from "../src/model";

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
    "PeriodBenchPlayed",
    "Before break10 min-",
    "After break20 min90 min",
  ]);
  const disclosure = within(breakdown)
    .getByText("Availability explanation")
    .closest("details")!;
  expect(disclosure.open).toBe(false);
  fireEvent.click(within(disclosure).getByText("Availability explanation"));
  expect(disclosure.open).toBe(true);
  expect(breakdown.textContent).toContain("Automatically inferred");
});

it("keeps missing recorded halves unavailable rather than splitting totals", () => {
  const breakdown = show({ ...base, prePlayed: 12, overridden: true }, vi.fn());
  const rows = within(breakdown).getAllByRole("row");
  expect(rows[1].textContent).toBe("Before break10 min12 min");
  expect(rows[2].textContent).toBe("After break20 minNot recorded");
  expect(breakdown.textContent).not.toContain("78 min");
  expect(breakdown.textContent).toContain("Officer adjustment applied");
  expect(
    within(breakdown).getByRole("button", { name: "Edit availability" }),
  ).toBeTruthy();
});

it("marks both played halves missing on older result documents", () => {
  const breakdown = show(base);
  expect(within(breakdown).getAllByText("Not recorded")).toHaveLength(2);
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
    "Before break20 min",
  );
});

it("shows attendance only when requested for a season row, preserving nightly and weekly scope", () => {
  const row = {
    ...base,
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
