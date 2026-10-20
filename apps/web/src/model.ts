import type { RecordedAvailability } from "./resultProvenance";

export type Page = "reports" | "overrides" | "results" | "roster" | "settings";
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

export const players: Player[] = [
  {
    id: "Example",
    name: "Exampleplayer",
    className: "Druid",
    role: "Healer",
    color: "#ffab68",
    pre: 72,
    post: 65,
  },
  {
    id: "quartz",
    name: "Quartz",
    className: "Warrior",
    role: "Tank",
    color: "#d4b88c",
    pre: 90,
    post: 80,
  },
  {
    id: "lunara",
    name: "Lunara",
    className: "Priest",
    role: "Healer",
    color: "#e3dfef",
    pre: 64,
    post: 52,
  },
  {
    id: "ash",
    name: "Ashenveil",
    className: "Warlock",
    role: "Damage",
    color: "#bdacff",
    pre: 60,
    post: 56,
  },
  {
    id: "bramble",
    name: "Bramble",
    className: "Hunter",
    role: "Damage",
    color: "#b4d889",
    pre: 79,
    post: 62,
  },
  {
    id: "vesper",
    name: "Vesper",
    className: "Rogue",
    role: "Damage",
    color: "#f1df8c",
    pre: 52,
    post: 59,
  },
  {
    id: "cinder",
    name: "Cinder",
    className: "Mage",
    role: "Damage",
    color: "#87cde7",
    pre: 66,
    post: 48,
  },
  {
    id: "solace",
    name: "Solace",
    className: "Paladin",
    role: "Healer",
    color: "#e9a1c3",
    pre: 0,
    post: 0,
  },
];
export const nightOptions = ["2026-10-15", "2026-10-14", "2026-10-08"];
export const demoCode = "aB3dE6gH9jK2mN5p";
export const initialInputs: Inputs = {
  reports: [
    {
      code: "kA3mP7rT2vW5xY8z",
      enabled: true,
      night: "2026-10-15",
      title: "Thursday · Mythic progression",
      fetched: true,
    },
    {
      code: "bD4fG7jL2nQ5sV8x",
      enabled: true,
      night: "2026-10-14",
      title: "Wednesday · Mythic progression",
      fetched: true,
    },
    {
      code: "cE5hJ8mN3pR6tW9y",
      enabled: true,
      night: "2026-10-08",
      title: "Thursday · Mythic progression",
      fetched: true,
    },
    {
      code: "dF6jK9nP4qS7vX2z",
      enabled: false,
      night: "2026-10-08",
      title: "Thursday · Alternate perspective",
      fetched: true,
    },
  ],
  adjustments: {},
  timings: {},
};
export const emptyAdjustment = (): Adjustment => ({
  pre: { mode: "automatic" },
  post: { mode: "automatic" },
  notes: "",
});
export const defaultTiming = (night: string): Timing => ({
  start: `${night}T19:00`,
  breakStart: `${night}T20:30`,
  breakEnd: `${night}T20:40`,
  end: `${night}T22:00`,
  notes: "",
});
export const signature = (inputs: Inputs) => JSON.stringify(inputs);
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
export function timingSpans(night: string, override?: Timing) {
  const defaults = defaultTiming(night);
  const t = override ?? defaults;
  const values = [
    t.start || defaults.start,
    t.breakStart || defaults.breakStart,
    t.breakEnd || defaults.breakEnd,
    t.end || defaults.end,
  ];
  // Wall-clock arithmetic for the demo only; production uses the bot's timezone logic.
  const ms = values.map((v) => Date.parse(`${v}Z`));
  if (
    ms.some((v) => !Number.isFinite(v)) ||
    ms.some((v, i) => i > 0 && v <= ms[i - 1])
  )
    throw new Error(
      "Timing must follow this order: raid start, break start, break end, raid end.",
    );
  if ((ms[3] - ms[0]) / 60_000 > 720)
    throw new Error("Keep the demo raid window within 12 hours.");
  return {
    pre: Math.round((ms[1] - ms[0]) / 60_000),
    post: Math.round((ms[3] - ms[2]) / 60_000),
    effective: Object.fromEntries(
      Object.entries(defaults).map(([k, v]) => [k, t[k as keyof Timing] || v]),
    ) as Timing,
  };
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
export function calculate(
  inputs: Inputs,
  completedAt = new Date().toISOString(),
): Results {
  const included = inputs.reports.filter((r) => r.enabled);
  const nights = [...new Set(included.map((r) => r.night))]
    .sort()
    .reverse()
    .map((night) => {
      const span = timingSpans(night, inputs.timings[night]);
      const rows = players.map((p) => {
        const a =
          inputs.adjustments[adjustmentKey(night, p.id)] ?? emptyAdjustment();
        const error = validateAdjustment(a, span.pre, span.post);
        if (error) throw new Error(`${p.name}, ${formatDate(night)}: ${error}`);
        const playedPre = Math.min(p.pre, span.pre),
          playedPost = Math.min(p.post, span.post);
        const available = (
          half: Availability,
          duration: number,
          played: number,
        ) =>
          half.mode === "partial"
            ? half.minutes!
            : half.mode === "unavailableFor"
              ? Math.max(0, duration - half.minutes!)
              : half.mode === "unavailable"
                ? 0
                : half.mode === "available"
                  ? duration
                  : played > 0
                    ? duration
                    : 0;
        const preBench = Math.max(
          0,
          available(a.pre, span.pre, playedPre) - playedPre,
        );
        const postBench = Math.max(
          0,
          available(a.post, span.post, playedPost) - playedPost,
        );
        return {
          id: p.id,
          played: playedPre + playedPost,
          prePlayed: playedPre,
          postPlayed: playedPost,
          bench: preBench + postBench,
          preBench,
          postBench,
          overridden: a.pre.mode !== "automatic" || a.post.mode !== "automatic",
          nights: playedPre + playedPost + preBench + postBench > 0 ? 1 : 0,
        };
      });
      return {
        night,
        duration: span.pre + span.post,
        timing: span.effective,
        players: rows,
        reports: included.filter((r) => r.night === night).length,
      };
    });
  const totals = players.map((p) => {
    const rows = nights.map((n) => n.players.find((r) => r.id === p.id)!);
    return {
      id: p.id,
      played: rows.reduce((s, r) => s + r.played, 0),
      prePlayed: rows.reduce((s, r) => s + r.prePlayed, 0),
      postPlayed: rows.reduce((s, r) => s + r.postPlayed, 0),
      bench: rows.reduce((s, r) => s + r.bench, 0),
      preBench: rows.reduce((s, r) => s + r.preBench, 0),
      postBench: rows.reduce((s, r) => s + r.postBench, 0),
      overridden: rows.some((r) => r.overridden),
      nights: rows.reduce((s, r) => s + r.nights, 0),
    };
  });
  return {
    completedAt,
    signature: signature(inputs),
    nights,
    players: totals,
    reportCount: included.length,
  };
}
export function freshState(): Saved {
  const inputs = structuredClone(initialInputs);
  const results = calculate(inputs, "2026-10-16T05:18:00.000Z");
  // Sample-only illustration. Connected results use the worker's publication;
  // missing attendance in connected mode is never replaced with these values.
  const sampleAttendance: AttendanceProjection = {
    played: 411,
    bench: 99,
    possible: 510,
    rate: 1,
    weeks: [
      { week: "2026-10-06", status: "PB" },
      { week: "2026-10-13", status: "PB" },
    ],
  };
  results.players.find((player) => player.id === "Example")!.attendance =
    sampleAttendance;
  results.players.find((player) => player.id === "solace")!.attendance = {
    played: 0,
    bench: 0,
    possible: 510,
    rate: 0,
    weeks: [
      { week: "2026-10-06", status: "O" },
      { week: "2026-10-13", status: "O" },
    ],
  };
  return {
    inputs,
    results,
    history: [
      {
        id: "sample-run",
        at: "2026-10-16T05:18:00.000Z",
        status: "succeeded",
        message: "3 reports processed · 3 raid nights",
      },
    ],
  };
}
export function formatDate(night: string, long = false) {
  return new Intl.DateTimeFormat("en-US", {
    month: long ? "long" : "short",
    day: "numeric",
    ...(long ? ({ weekday: "long" } as const) : {}),
    timeZone: "UTC",
  }).format(new Date(`${night}T12:00:00Z`));
}
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
  `${Math.floor(value / 60)}h ${String(value % 60).padStart(2, "0")}m`;

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
