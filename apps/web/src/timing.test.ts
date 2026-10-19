import { describe, expect, it } from "vitest";
import { blankTiming, validateTiming } from "./timing";
import { wallTime } from "./model";
const detected = {
  ...blankTiming(),
  start: "2026-10-14T19:00",
  breakStart: "2026-10-14T20:00",
  breakEnd: "2026-10-14T20:15",
  end: "2026-10-14T22:00",
};
describe("Pacific timing validation", () => {
  it("allows automatic fields and notes before timing is detected", () => {
    expect(
      validateTiming(
        { ...blankTiming(), notes: "Wait for detection" },
        blankTiming(),
      ),
    ).toBeNull();
    expect(validateTiming(blankTiming(), detected)).toBeNull();
    expect(wallTime("2026-10-14T19:00")).toBe("7:00 PM");
  });
  it("requires both explicit break boundaries", () => {
    expect(
      validateTiming(
        { ...blankTiming(), breakStart: detected.breakStart },
        detected,
      ),
    ).toContain("both break boundaries");
    expect(
      validateTiming(
        { ...blankTiming(), breakEnd: detected.breakEnd },
        detected,
      ),
    ).toContain("both break boundaries");
  });
  it("checks explicit and detected order including equal times", () => {
    expect(
      validateTiming(
        { ...blankTiming(), start: detected.end, end: detected.end },
        blankTiming(),
      ),
    ).toContain("Timing must follow");
    expect(
      validateTiming({ ...blankTiming(), start: "2026-10-14T20:05" }, detected),
    ).toContain("Timing must follow");
    expect(
      validateTiming({ ...blankTiming(), start: "2026-10-14T19:05" }, detected),
    ).toBeNull();
  });
  it("accepts overnight raids and enforces the worker's explicit 12-hour limit", () => {
    const value = {
      ...blankTiming(),
      start: "2026-10-14T19:00",
      end: "2026-10-15T01:00",
    };
    expect(validateTiming(value, blankTiming())).toBeNull();
    expect(
      validateTiming({ ...value, end: "2026-10-15T07:00" }, blankTiming()),
    ).toBeNull();
    expect(
      validateTiming({ ...value, end: "2026-10-15T07:01" }, blankTiming()),
    ).toContain("12 hours");
  });
  it("rejects invalid calendar dates, minute formats and excessive notes", () => {
    for (const start of [
      "2026-02-30T19:00",
      "2026-10-14T25:00",
      "2026-10-14T19:00:30",
      "invalid",
    ])
      expect(
        validateTiming({ ...blankTiming(), start }, blankTiming()),
      ).toContain("valid date and Pacific time");
    expect(
      validateTiming(
        { ...blankTiming(), notes: "a".repeat(1001) },
        blankTiming(),
      ),
    ).toContain("1000");
  });
  it("rejects Pacific spring gaps and fall ambiguity", () => {
    expect(
      validateTiming(
        { ...blankTiming(), start: "2026-03-08T02:30" },
        blankTiming(),
      ),
    ).toContain("does not exist");
    expect(
      validateTiming(
        { ...blankTiming(), start: "2026-11-01T01:30" },
        blankTiming(),
      ),
    ).toContain("ambiguous");
    for (const start of [
      "2026-03-08T01:30",
      "2026-03-08T03:30",
      "2026-11-01T00:30",
      "2026-11-01T02:30",
    ])
      expect(
        validateTiming({ ...blankTiming(), start }, blankTiming()),
      ).toBeNull();
  });
  it("uses elapsed Pacific time across offset changes for the explicit duration limit", () => {
    expect(
      validateTiming(
        {
          ...blankTiming(),
          start: "2026-10-31T19:00",
          end: "2026-11-01T07:00",
        },
        blankTiming(),
      ),
    ).toContain("12 hours");
    expect(
      validateTiming(
        {
          ...blankTiming(),
          start: "2026-03-07T19:00",
          end: "2026-03-08T08:00",
        },
        blankTiming(),
      ),
    ).toBeNull();
  });
});
