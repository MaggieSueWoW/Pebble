import type { PlayerResult, WeekResult } from "./model";

type StoredWeekPlayer = {
  main: string;
  played_min: number;
  bench_min: number;
  bench_pre_min: number;
  bench_post_min: number;
};

/** Adapt the worker's public weekly projection without inferring eligibility or time. */
export function readWeekResults(
  weeks: { week: string; players: StoredWeekPlayer[] }[],
  players: PlayerResult[],
): WeekResult[] {
  const metadata = new Map(players.map((p) => [p.name, p]));
  return weeks
    .map(({ week, players: rows }) => ({
      week,
      players: rows.map((row) => {
        const p = metadata.get(row.main);
        return {
          id: p?.id ?? row.main,
          name: row.main,
          className: p?.className,
          role: p?.role,
          color: p?.color,
          played: row.played_min,
          bench: row.bench_min,
          preBench: row.bench_pre_min,
          postBench: row.bench_post_min,
        };
      }),
    }))
    .sort((a, b) => b.week.localeCompare(a.week));
}
