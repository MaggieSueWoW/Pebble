import type { Timing } from "../../shared/model";
export const blankTiming = (): Timing => ({
  start: "",
  breakStart: "",
  breakEnd: "",
  end: "",
  notes: "",
});
export const timingFields = [
  { key: "start", label: "Mythic start" },
  { key: "breakStart", label: "Break start" },
  { key: "breakEnd", label: "Break end" },
  { key: "end", label: "Mythic end" },
] as const;
const pacific = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/Los_Angeles",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  hourCycle: "h23",
});
const hour = 60 * 60 * 1000;
function wallAt(ms: number) {
  const p = Object.fromEntries(
    pacific.formatToParts(ms).map(({ type, value }) => [type, value]),
  );
  return `${p.year}-${p.month}-${p.day}T${p.hour}:${p.minute}`;
}
/** Resolve a Pacific wall minute without relying on the browser's timezone. */
function pacificMinute(value: string) {
  const naive = Date.parse(`${value}:00Z`);
  if (
    !/^\d{4}-\d{2}-\d{2}T([01]\d|2[0-3]):[0-5]\d$/.test(value) ||
    !Number.isFinite(naive) ||
    new Date(naive).toISOString().slice(0, 16) !== value
  )
    throw new Error("Enter a valid date and Pacific time.");
  // Nearby probes discover both offsets across a daylight-saving transition.
  const offsets = new Set(
    [-36, 0, 36].map((h) => {
      const probe = naive + h * hour;
      return Date.parse(`${wallAt(probe)}:00Z`) - probe;
    }),
  );
  const matches = [...offsets]
    .map((offset) => naive - offset)
    .filter((ms) => wallAt(ms) === value);
  if (!matches.length)
    throw new Error(
      "This Pacific time does not exist because of daylight saving time.",
    );
  if (matches.length > 1)
    throw new Error(
      "This Pacific time is ambiguous because of daylight saving time.",
    );
  return matches[0];
}
export function validateTiming(value: Timing, detected: Timing): string | null {
  if (Boolean(value.breakStart) !== Boolean(value.breakEnd))
    return "Set both break boundaries or leave both automatic.";
  if (value.notes.length > 1000)
    return "Keep timing notes within 1000 characters.";
  const explicit: number[] = [];
  for (const { key, label } of timingFields) {
    if (!value[key]) continue;
    try {
      explicit.push(pacificMinute(value[key]));
    } catch (cause) {
      return `${label}: ${(cause as Error).message}`;
    }
  }
  const effective = timingFields
    .map(({ key }) => value[key] || detected[key])
    .filter(Boolean);
  if (
    explicit.some((ms, i) => i > 0 && ms <= explicit[i - 1]) ||
    effective.some((wall, i) => i > 0 && wall <= effective[i - 1])
  )
    return "Timing must follow start, break start, break end, end.";
  if (
    explicit.length > 1 &&
    explicit[explicit.length - 1] - explicit[0] > 12 * hour
  )
    return "Raid timing cannot span more than 12 hours.";
  return null;
}
