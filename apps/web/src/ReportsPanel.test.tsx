// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { ReportsPanel } from "./ReportsPanel";
import { useReportEditor } from "./useReportEditor";
import type { Report } from "./model";
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
  const editor = useReportEditor(reports, {}, save);
  return <ReportsPanel reports={reports} editor={editor} demo={false} />;
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
