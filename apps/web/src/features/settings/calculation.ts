export type CalculationSettings = {
  tz: "America/Los_Angeles";
  breakWindowStart: string;
  breakWindowEnd: string;
  minBreakMinutes: number;
  maxBreakMinutes: number;
  defaultMythicStart: string;
  postExtensionMinutes: number;
};
export const defaultCalculation = (): CalculationSettings => ({
  tz: "America/Los_Angeles",
  breakWindowStart: "20:50",
  breakWindowEnd: "21:30",
  minBreakMinutes: 10,
  maxBreakMinutes: 30,
  defaultMythicStart: "",
  postExtensionMinutes: 5,
});
export function validateCalculation(value: CalculationSettings): string | null {
  const clock = /^(?:[01]\d|2[0-3]):[0-5]\d$/;
  if (value.tz !== "America/Los_Angeles")
    return "Calculation times must use Pacific time.";
  if (
    !clock.test(value.breakWindowStart) ||
    !clock.test(value.breakWindowEnd) ||
    (value.defaultMythicStart !== "" && !clock.test(value.defaultMythicStart))
  )
    return "Use HH:MM for calculation times. Both break window boundaries are required.";
  if (value.breakWindowStart >= value.breakWindowEnd)
    return "Break window end must follow start.";
  if (
    ![value.minBreakMinutes, value.maxBreakMinutes].every(
      (n) => Number.isInteger(n) && n >= 1 && n <= 120,
    )
  )
    return "Break durations must be whole minutes from 1 to 120.";
  if (value.minBreakMinutes > value.maxBreakMinutes)
    return "Minimum break duration must not exceed maximum.";
  if (
    !Number.isFinite(value.postExtensionMinutes) ||
    value.postExtensionMinutes < 0 ||
    value.postExtensionMinutes > 60
  )
    return "Post extension must be between 0 and 60 minutes.";
  return null;
}
