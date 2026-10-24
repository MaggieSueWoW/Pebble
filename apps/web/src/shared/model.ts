import type { RecordedAvailability } from "../features/results/resultProvenance";

export type Page =
  | "reports"
  | "review"
  | "overrides"
  | "results"
  | "roster"
  | "settings"
  | "officers";
export type Mode =
  "automatic" | "available" | "unavailable" | "partial" | "unavailableFor";
export type Availability = { mode: Mode; minutes?: number };
export type Adjustment = {
  pre: Availability;
  post: Availability;
  notes: string;
};
export type Timing = {
  start: string;
  breakStart: string;
  breakEnd: string;
  end: string;
  notes: string;
};
export type Report = {
  code: string;
  enabled: boolean;
  night: string;
  title: string;
  fetched: boolean;
  checkedAt?: string;
  notes?: string;
};
export type Inputs = {
  reports: Report[];
  adjustments: Record<string, Adjustment>;
  timings: Record<string, Timing>;
};
export type Player = {
  id: string;
  name: string;
  className: string;
  role: string;
  color: string;
  pre: number;
  post: number;
};
/** Display only known roster metadata, without empty separators. */
export function playerDetails(player: {
  className?: string;
  role?: string;
}): string {
  return [player.className, player.role]
    .map((value) => value?.trim() ?? "")
    .filter((value) => value && value.toLowerCase() !== "unknown")
    .join(" · ");
}

export type AttendanceProjection = {
  played: number;
  bench: number;
  possible: number;
  rate: number | null;
  weeks: { week: string; status: string }[];
};
export type PlayerResult = {
  /** Captured nightly calculation provenance; omitted on aggregates/older results. */
  availability?: RecordedAvailability;
  attendance?: AttendanceProjection;
  /** Season-only established ranking; null means explicitly excluded. */
  ranking?: {
    rank: number;
    played: number;
    bench: number;
    ratio: number | null;
  } | null;
  name?: string;
  className?: string;
  role?: string;
  color?: string;
  id: string;
  played: number;
  /** Recorded worker breakdown; older publications can omit these fields. */
  prePlayed?: number;
  postPlayed?: number;
  bench: number;
  preBench: number;
  postBench: number;
  overridden: boolean;
  nights: number;
};
export type NightResult = {
  night: string;
  duration: number;
  timing: Timing;
  players: PlayerResult[];
  reports: number;
};
/** Weekly minutes are published by the worker, not recomputed in the browser. */
export type WeeklyPlayerResult = Pick<
  PlayerResult,
  | "id"
  | "name"
  | "className"
  | "role"
  | "color"
  | "played"
  | "bench"
  | "preBench"
  | "postBench"
>;
export type WeekResult = { week: string; players: WeeklyPlayerResult[] };
export type Results = {
  weeks?: WeekResult[];
  warnings?: string[];
  completedAt: string;
  signature: string;
  nights: NightResult[];
  players: PlayerResult[];
  reportCount: number;
};
export type History = {
  id: string;
  at: string;
  status: "succeeded" | "failed";
  message: string;
};
export type Saved = { inputs: Inputs; results: Results; history: History[] };
export type Parsed = {
  raw: string;
  code: string;
  state: "ready" | "existing" | "disabled" | "duplicate" | "invalid";
  message: string;
};

export const emptyAdjustment = (): Adjustment => ({
  pre: { mode: "automatic" },
  post: { mode: "automatic" },
  notes: "",
});
export const adjustmentKey = (night: string, id: string) => `${night}/${id}`;
export function parseReports(text: string, reports: Report[]): Parsed[] {
  const seen = new Set<string>();
  return text
    .split(/\r?\n/)
    .map((s) => s.trim())
    .filter(Boolean)
    .map((raw) => {
      let code = raw;
      if (/^https?:\/\//i.test(raw)) {
        try {
          const url = new URL(raw);
          if (
            !["www.warcraftlogs.com", "warcraftlogs.com"].includes(
              url.hostname,
            ) ||
            url.protocol !== "https:"
          )
            throw new Error();
          const match = url.pathname.match(/^\/reports\/([A-Za-z0-9]{16})\/?$/);
          if (!match) throw new Error();
          code = match[1];
        } catch {
          return {
            raw,
            code: "",
            state: "invalid",
            message:
              "Use a Warcraft Logs HTTPS report URL or 16-character code.",
          };
        }
      }
      if (!/^[A-Za-z0-9]{16}$/.test(code))
        return {
          raw,
          code: "",
          state: "invalid",
          message: "Report codes contain 16 letters and numbers.",
        };
      if (seen.has(code))
        return {
          raw,
          code,
          state: "duplicate",
          message: "Repeated in this list · will be skipped",
        };
      seen.add(code);
      const found = reports.find((r) => r.code === code);
      return {
        raw,
        code,
        state: found ? (found.enabled ? "existing" : "disabled") : "ready",
        message: found
          ? found.enabled
            ? "Already added · will be skipped"
            : "Already added · currently excluded"
          : "Ready to add",
      };
    });
}
export function validateAdjustment(
  a: Adjustment,
  pre: number,
  post: number,
): string | null {
  for (const [label, half, bound] of [
    ["Before break", a.pre, pre],
    ["After break", a.post, post],
  ] as const) {
    if (
      ["partial", "unavailableFor"].includes(half.mode) &&
      (!Number.isInteger(half.minutes) ||
        half.minutes! <= 0 ||
        half.minutes! > (half.mode === "unavailableFor" ? 720 : bound))
    )
      return `${label}: enter whole minutes between 1 and ${half.mode === "unavailableFor" ? 720 : bound}.`;
  }
  return null;
}
export function formatDate(night: string, long = false) {
  return new Intl.DateTimeFormat("en-US", {
    month: long ? "long" : "short",
    day: "numeric",
    ...(long ? ({ weekday: "long" } as const) : {}),
    timeZone: "UTC",
  }).format(new Date(`${night}T12:00:00Z`));
}
export const formatRunDate = (iso: string) =>
  new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Los_Angeles",
    month: "short",
    day: "numeric",
  }).format(new Date(iso));

export const formatTime = (iso: string) =>
  new Intl.DateTimeFormat("en-US", {
    hour: "numeric",
    minute: "2-digit",
    timeZone: "America/Los_Angeles",
  }).format(new Date(iso));
export const wallTime = (value: string) =>
  !value
    ? "Not detected"
    : new Intl.DateTimeFormat("en-US", {
        hour: "numeric",
        minute: "2-digit",
        timeZone: "UTC",
      }).format(new Date(`${value}Z`));
export const minutes = (value: number) =>
  value === 0
    ? "-"
    : value < 60
      ? `${value}m`
      : `${Math.floor(value / 60)}h ${String(value % 60).padStart(2, "0")}m`;

export function stableJSON(value: unknown): string {
  if (Array.isArray(value)) return "[" + value.map(stableJSON).join(",") + "]";
  if (value !== null && typeof value === "object")
    return (
      "{" +
      Object.entries(value)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([k, v]) => JSON.stringify(k) + ":" + stableJSON(v))
        .join(",") +
      "}"
    );
  return JSON.stringify(value);
}
