import { expect, it } from "vitest";
import { defaultCalculation, validateCalculation } from "./calculation";
it("uses worker defaults and accepts optional timing and fractional extension", () => {
  expect(validateCalculation(defaultCalculation())).toBeNull();
  expect(
    validateCalculation({
      ...defaultCalculation(),
      defaultMythicStart: "19:30",
      postExtensionMinutes: 2.5,
    }),
  ).toBeNull();
});
it("rejects incomplete times, reversed windows, noninteger break bounds and extension bounds", () => {
  for (const change of [
    { breakWindowStart: "" },
    { breakWindowEnd: "25:00" },
    { defaultMythicStart: "9:30" },
    { breakWindowEnd: "20:50" },
    { minBreakMinutes: 31 },
    { minBreakMinutes: 0 },
    { maxBreakMinutes: 121 },
    { minBreakMinutes: 1.5 },
    { postExtensionMinutes: NaN },
    { postExtensionMinutes: -1 },
    { postExtensionMinutes: 61 },
  ])
    expect(
      validateCalculation({ ...defaultCalculation(), ...change }),
    ).not.toBeNull();
});
