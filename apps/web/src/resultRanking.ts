import type { PlayerResult } from "./model";

type Row = {
  id: string;
  name?: string;
  played: number;
  bench: number;
  ranking?: PlayerResult["ranking"];
};

/** Compare Unicode names by code point, matching the established binary name order. */
function compareNames(a: string, b: string): number {
  const left = Array.from(a, (char) => char.codePointAt(0)!);
  const right = Array.from(b, (char) => char.codePointAt(0)!);
  for (let i = 0; i < Math.min(left.length, right.length); i++) {
    if (left[i] !== right[i]) return left[i] - right[i];
  }
  return left.length - right.length;
}

/** Use recorded season eligibility/order and the existing period visibility. */
export function orderResultRows<T extends Row>(
  rows: T[],
  period: "season" | "night" | "week",
  name: (row: T) => string = (row) => row.name ?? row.id,
): (T & Row)[] {
  return rows
    .filter((row) => {
      if (period === "season" && row.ranking === null) return false;
      return (
        period === "week" ||
        (period === "season" && row.ranking != null) ||
        row.played + row.bench > 0
      );
    })
    .sort((a, b) => {
      if (period === "season" && a.ranking && b.ranking)
        return a.ranking.rank - b.ranking.rank;
      // The established ranking uses binary name ordering, not locale-dependent collation.
      return a.bench - b.bench || compareNames(name(a), name(b));
    });
}
