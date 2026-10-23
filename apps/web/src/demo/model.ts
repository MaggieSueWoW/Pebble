// Illustrative sample data and calculations; live results come from the worker.
import {
  adjustmentKey,
  emptyAdjustment,
  formatDate,
  validateAdjustment,
  type Player,
  type Inputs,
  type Results,
  type Saved,
  type Timing,
  type Availability,
  type AttendanceProjection,
} from "../model";

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

export const defaultTiming = (night: string): Timing => ({
  start: `${night}T19:00`,
  breakStart: `${night}T20:30`,
  breakEnd: `${night}T20:40`,
  end: `${night}T22:00`,
  notes: "",
});
export const signature = (inputs: Inputs) => JSON.stringify(inputs);

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
