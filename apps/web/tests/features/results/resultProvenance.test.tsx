// @vitest-environment jsdom
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";
import { AvailabilityExplanation } from "../../../src/features/results/AvailabilityExplanation";
import { NightDiagnostics } from "../../../src/features/results/NightDiagnostics";
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
  ).toContain("Recorded available · Input: Partial availability");
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
  expect(screen.getByText("Configured")).toBeTruthy();
  expect(screen.getByText("Overridden")).toBeTruthy();
  expect(screen.getAllByText("No break recorded")).toHaveLength(1);
  expect(screen.queryByText("Unmapped characters")).toBeNull();
  expect(screen.queryByText("Recorded break")).toBeNull();
  expect(screen.getByText("8")).toBeTruthy();
});
it("labels missing older diagnostics as not recorded", () => {
  render(<NightDiagnostics night="2026-10-15" />);
  expect(screen.getAllByText("Source not recorded")).toBeTruthy();
});

it("keeps each timing source below its value and rounds display durations", () => {
  render(
    <NightDiagnostics
      night="2026-10-21"
      recorded={{
        night: "2026-10-21",
        duration: 96.86,
        reports: 1,
        players: [],
        timing: {
          start: "2026-10-21T20:19",
          end: "2026-10-21T22:26",
          breakStart: "2026-10-21T20:50",
          breakEnd: "2026-10-21T21:26",
          notes: "",
        },
      }}
      diagnostic={{
        reportCount: 1,
        mythicFights: 16,
        unmappedCharacters: 0,
        breakMinutes: 35.8,
        timing: { start: "detected", end: "override", break: "detected" },
      }}
    />,
  );
  expect(screen.getByText("8:19 PM").textContent).toBe("8:19 PMDetected");
  expect(screen.getByText("10:26 PM").textContent).toBe("10:26 PMOverridden");
  expect(screen.getByText(/8:50 PM – 9:26 PM/).textContent).toContain(
    "(36 min)",
  );
  expect(screen.getByText("1h 37m")).toBeTruthy();
});
