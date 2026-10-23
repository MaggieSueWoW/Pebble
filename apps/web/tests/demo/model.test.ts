import { describe, expect, it } from "vitest";
import { calculate, freshState, timingSpans } from "../../src/demo/model";
import {
  adjustmentKey,
  emptyAdjustment,
  validateAdjustment,
} from "../../src/shared/model";

describe("saved inputs versus completed results", () => {
  it("removes a disabled night and all of its contribution without modifying the old results", () => {
    const { inputs, results } = freshState();
    inputs.reports[0].enabled = false;
    const next = calculate(inputs);
    expect(next.nights).toHaveLength(2);
    expect(next.players.find((p) => p.id === "quartz")!.played).toBe(340);
    expect(results.nights).toHaveLength(3);
    expect(results.players.find((p) => p.id === "quartz")!.played).toBe(510);
  });
  it("credits an absent roster member when availability is explicitly adjusted", () => {
    const { inputs, results } = freshState();
    const a = emptyAdjustment();
    a.post = { mode: "partial", minutes: 45 };
    inputs.adjustments[adjustmentKey("2026-10-15", "solace")] = a;
    expect(results.players.find((p) => p.id === "solace")!.bench).toBe(0);
    expect(
      calculate(inputs).players.find((p) => p.id === "solace")!.bench,
    ).toBe(45);
  });
  it("rejects partial minutes beyond the actual half instead of updating the result", () => {
    const a = emptyAdjustment();
    a.pre = { mode: "partial", minutes: 91 };
    expect(validateAdjustment(a, 90, 80)).toMatch(/between 1 and 90/);
    const { inputs } = freshState();
    inputs.adjustments[adjustmentKey("2026-10-15", "Example")] = a;
    expect(() => calculate(inputs)).toThrow(/Exampleplayer/);
  });
  it("allows an empty result when every report is excluded, rather than retaining stale totals", () => {
    const { inputs } = freshState();
    inputs.reports.forEach((r) => {
      r.enabled = false;
    });
    const result = calculate(inputs);
    expect(result.nights).toEqual([]);
    expect(result.players.every((r) => r.bench === 0 && r.played === 0)).toBe(
      true,
    );
  });
});

describe("timing", () => {
  it("supports midnight crossings and rejects reversed break boundaries", () => {
    expect(
      timingSpans("2026-10-15", {
        start: "2026-10-15T23:00",
        breakStart: "2026-10-16T00:30",
        breakEnd: "2026-10-16T00:40",
        end: "2026-10-16T02:00",
        notes: "",
      }),
    ).toMatchObject({ pre: 90, post: 80 });
    expect(() =>
      timingSpans("2026-10-15", {
        start: "",
        breakStart: "2026-10-15T21:00",
        breakEnd: "2026-10-15T20:00",
        end: "",
        notes: "",
      }),
    ).toThrow(/Timing must follow/);
  });
});

describe("sample unavailable time", () => {
  it("credits the remainder of a half when unavailable for N minutes", () => {
    const { inputs } = freshState();
    const a = emptyAdjustment();
    a.post = { mode: "unavailableFor", minutes: 35 };
    inputs.adjustments[adjustmentKey("2026-10-15", "solace")] = a;
    expect(
      calculate(inputs).players.find((p) => p.id === "solace")!.bench,
    ).toBe(45);
  });
});
