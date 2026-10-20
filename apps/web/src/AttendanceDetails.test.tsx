// @vitest-environment jsdom
import { cleanup, render, screen, within } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";
import { AttendanceDetails } from "./AttendanceDetails";
import type { AttendanceProjection } from "./model";
afterEach(cleanup);
const value: AttendanceProjection = {
  played: 100,
  bench: 50,
  possible: 200,
  rate: 0.75,
  weeks: [
    { week: "2026-10-13", status: "PBO" },
    { week: "2026-10-06", status: "" },
  ],
};
it("shows published helper minutes and rate without deriving from aggregate played share", () => {
  render(<AttendanceDetails attendance={{ ...value, rate: 0.12345 }} />);
  const attendance = screen.getByRole("region", { name: "Attendance" });
  expect(attendance.textContent).toContain("Attendance rate12.3%");
  expect(attendance.textContent).toContain("Eligible played100 min");
  expect(attendance.textContent).toContain("Eligible bench50 min");
  expect(attendance.textContent).toContain("Possible minutes200 min");
  expect(attendance.textContent).not.toContain("75%");
  expect(attendance.textContent).toContain("differs from the played share");
});
it("orders weekly records, expands P/B/O accessibly and identifies empty statuses", () => {
  render(<AttendanceDetails attendance={value} />);
  const rows = within(
    screen.getByRole("table", { name: "Weekly attendance" }),
  ).getAllByRole("row");
  expect(rows[1].textContent).toContain("October 6");
  expect(rows[1].textContent).toContain("No applicable event");
  expect(rows[2].textContent).toContain("October 13");
  expect(screen.getByLabelText("played, benched, out").textContent).toBe("PBO");
  expect(screen.getByText(/P = played/).textContent).toContain(
    "B = benched · O = out",
  );
  expect(value.weeks[0].week).toBe("2026-10-13");
});
it("keeps a missing projection unavailable without a zero rate or eligibility guess", () => {
  render(<AttendanceDetails />);
  expect(screen.getByText(/Attendance unavailable/)).toBeTruthy();
  expect(screen.queryByText("0%")).toBeNull();
  expect(screen.queryByText(/ineligible|left the roster/i)).toBeNull();
  expect(screen.queryByRole("table")).toBeNull();
});
it.each([
  { possible: 0, rate: null },
  { possible: 0, rate: 0 },
  { possible: 200, rate: null },
])(
  "does not invent a percentage for missing or zero possible time: %o",
  (projection) => {
    render(<AttendanceDetails attendance={{ ...value, ...projection }} />);
    expect(screen.getByText("Unavailable", { exact: true })).toBeTruthy();
    expect(screen.queryByText("0%", { exact: true })).toBeNull();
  },
);
it("distinguishes a recorded zero attendance rate from unavailable data", () => {
  render(
    <AttendanceDetails
      attendance={{ ...value, played: 0, bench: 0, rate: 0, weeks: [] }}
    />,
  );
  expect(screen.getByText("0%", { exact: true })).toBeTruthy();
  expect(screen.getAllByText("0 min", { exact: true })).toHaveLength(2);
  expect(screen.getByText("No weekly attendance was published.")).toBeTruthy();
});
