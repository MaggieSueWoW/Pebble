// @vitest-environment jsdom
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";
import { AvailabilityExplanation } from "./AvailabilityExplanation";
import { NightDiagnostics } from "./NightDiagnostics";
afterEach(cleanup);
it("explains partial half flags and whole-row source without inventing full-half time", () => {
  render(
    <AvailabilityExplanation
      availability={{
        pre: { available: true, mode: "partial" },
        post: { available: false, mode: "automatic" },
        source: "override",
      }}
    />,
  );
  expect(
    screen.getByRole("region", { name: "Recorded availability explanation" })
      .textContent,
  ).toContain("Available flag · Input: Partial availability");
  expect(screen.getByText("Officer adjustment")).toBeTruthy();
  expect(screen.getByText(/overall source describes the row/)).toBeTruthy();
});
it("does not infer source or availability for old result rows", () => {
  render(<AvailabilityExplanation />);
  expect(
    screen.getByText("Availability source not recorded for this result."),
  ).toBeTruthy();
  expect(screen.queryByText("Automatic")).toBeNull();
});
it("shows private aggregate diagnostics with timing sources and no invented break", () => {
  render(
    <NightDiagnostics
      night="2026-10-15"
      diagnostic={{
        reportCount: 2,
        mythicFights: 8,
        unmappedCharacters: 0,
        breakMinutes: null,
        timing: { start: "configured", end: "override", break: "none" },
      }}
    />,
  );
  expect(screen.getByText("Configured default")).toBeTruthy();
  expect(screen.getByText("Officer timing override")).toBeTruthy();
  expect(screen.getAllByText("No break recorded")).toHaveLength(2);
  expect(screen.getByText("0")).toBeTruthy();
});
it("labels missing older diagnostics as not recorded", () => {
  render(<NightDiagnostics night="2026-10-15" />);
  expect(
    screen.getByText("No recorded diagnostics for this night."),
  ).toBeTruthy();
});
