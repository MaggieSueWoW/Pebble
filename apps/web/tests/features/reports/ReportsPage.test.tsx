// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { ReportsPage } from "../../../src/features/reports/ReportsPage";
import { useReportEditor } from "../../../src/features/reports/useReportEditor";
import type { Report } from "../../../src/shared/model";
const code = "AAAAAAAAAAAAAAAA";
const report: Report = {
  code,
  enabled: true,
  fetched: true,
  title: "Example raid",
  night: "2026-10-15",
};
const save = vi.fn().mockResolvedValue(undefined);
function Panel({ reports }: { reports: Report[] }) {
  const editor = useReportEditor(reports, {}, save, vi.fn());
  return (
    <ReportsPage
      reports={reports}
      editor={editor}
      sampleReportLink={undefined}
    />
  );
}
afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});
it("explains an empty report list alongside the existing add control", () => {
  render(<Panel reports={[]} />);
  expect(
    screen.getByText(/No reports yet. Paste a Warcraft Logs link/),
  ).toBeTruthy();
  expect(
    screen.getByRole("textbox", { name: "Warcraft Logs reports" }),
  ).toBeTruthy();
  expect(screen.queryByRole("switch")).toBeNull();
});
it.each([
  ["2026-10-16T02:05:00.000Z", "Oct 15, 2026, 7:05 PM PDT"],
  ["2026-01-16T03:05:00.000Z", "Jan 15, 2026, 7:05 PM PST"],
])("shows recorded %s in Pacific time", (checkedAt, expected) => {
  const { container } = render(<Panel reports={[{ ...report, checkedAt }]} />);
  expect(screen.getByText(expected)).toBeTruthy();
  expect(container.querySelector("time")?.dateTime).toBe(checkedAt);
  expect(save).not.toHaveBeenCalled();
});
it("uses code/title and timestamp fallbacks while metadata updates preserve selection edits", () => {
  const { rerender, container } = render(
    <Panel reports={[{ ...report, title: "  " }]} />,
  );
  expect(screen.getByRole("link", { name: code }).getAttribute("href")).toBe(
    `https://www.warcraftlogs.com/reports/${code}`,
  );
  expect(screen.getByText("Last checked unavailable")).toBeTruthy();
  fireEvent.click(screen.getByRole("switch", { name: `Include ${code}` }));
  rerender(
    <Panel reports={[{ ...report, checkedAt: "2026-10-16T02:05:00.000Z" }]} />,
  );
  expect(screen.getByRole("link", { name: "Example raid" })).toBeTruthy();
  expect(screen.getByRole("switch").getAttribute("aria-checked")).toBe("false");
  expect(screen.getByText(/1 unsaved selection change/)).toBeTruthy();
  rerender(<Panel reports={[{ ...report, checkedAt: "invalid" }]} />);
  expect(screen.getByText("Last checked unavailable")).toBeTruthy();
  expect(container.querySelector("time")).toBeNull();
  expect(save).not.toHaveBeenCalled();
});

it("sorts raid nights oldest first, reverses on click and keeps unfetched reports last", () => {
  const reports = [
    { ...report, code: "CCCCCCCCCCCCCCCC", night: "2026-10-21" },
    { ...report, code: "BBBBBBBBBBBBBBBB", night: "", fetched: false },
    { ...report, code: "DDDDDDDDDDDDDDDD", night: "2026-10-14" },
    {
      ...report,
      code: "AAAAAAAAAAAAAAAA",
      night: "2026-10-14",
      enabled: false,
    },
  ];
  render(<Panel reports={reports} />);
  const order = () =>
    screen
      .getAllByRole("switch")
      .map((button) => button.getAttribute("aria-label"));
  const header = screen.getByRole("columnheader", { name: "Raid night (PT)" });
  expect(header.getAttribute("aria-sort")).toBe("descending");
  expect(order()).toEqual([
    "Include CCCCCCCCCCCCCCCC",
    "Include AAAAAAAAAAAAAAAA",
    "Include DDDDDDDDDDDDDDDD",
    "Include BBBBBBBBBBBBBBBB",
  ]);
  fireEvent.click(screen.getByRole("button", { name: "Raid night (PT)" }));
  expect(header.getAttribute("aria-sort")).toBe("ascending");
  expect(order()).toEqual([
    "Include AAAAAAAAAAAAAAAA",
    "Include DDDDDDDDDDDDDDDD",
    "Include CCCCCCCCCCCCCCCC",
    "Include BBBBBBBBBBBBBBBB",
  ]);
  fireEvent.click(screen.getByRole("button", { name: "Raid night (PT)" }));
  expect(header.getAttribute("aria-sort")).toBe("descending");
  expect(save).not.toHaveBeenCalled();
});
