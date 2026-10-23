import { useMemo } from "react";
import type { DocumentData } from "firebase/firestore";
import {
  adjustmentKey,
  type Inputs,
  type Results,
  type Player,
} from "../../shared/model";
import {
  defaultCalculation,
  type CalculationSettings,
} from "../../features/settings/calculation";
import type { RosterEntry } from "../../features/roster/roster";
import type { CharacterEntry } from "../../features/roster/characters";
import { dataOnly, timestampISO } from "./documents";
import type { Versions } from "./types";
import type { PrivateState } from "./usePrivateState";

/** Convert Firestore snapshots to the existing application-facing view models. */
export function useWorkspaceData(
  state: PrivateState,
  results: Results,
  officer: boolean,
) {
  const { documents, runs } = state;
  const versions = useMemo(
    () =>
      Object.fromEntries(
        Object.entries(documents).flatMap(([name, values]) =>
          Object.entries(values).map(([id, d]) => [`${name}/${id}`, d.version]),
        ),
      ),
    [documents],
  ) as Versions;
  const summaries = useMemo(() => {
    const combined: Record<string, DocumentData> = {};
    [...runs]
      .reverse()
      .forEach((r) => Object.assign(combined, r.reportSummaries ?? {}));
    return combined;
  }, [runs]);
  const inputs: Inputs = useMemo(
    () => ({
      reports: Object.entries(documents.reports ?? {})
        .map(([code, d]) => ({
          code,
          enabled: d.enabled,
          notes: d.notes || "",
          night: summaries[code]?.night || "",
          title: summaries[code]?.title || "",
          fetched: Boolean(summaries[code]),
          checkedAt: timestampISO(summaries[code]?.checkedAt),
        }))
        .sort(
          (a, b) =>
            b.night.localeCompare(a.night) || a.code.localeCompare(b.code),
        ),
      adjustments: Object.fromEntries(
        Object.values(documents.availabilityOverrides ?? {}).map((d) => [
          adjustmentKey(d.nightId, d.playerId),
          { pre: d.pre, post: d.post, notes: d.notes ?? "" },
        ]),
      ),
      timings: Object.fromEntries(
        Object.entries(documents.nightOverrides ?? {}).map(([id, d]) => [
          id,
          {
            start: d.start,
            breakStart: d.breakStart,
            breakEnd: d.breakEnd,
            end: d.end,
            notes: d.notes ?? "",
          },
        ]),
      ),
    }),
    [documents, summaries],
  );
  const roster = Object.entries(documents.players ?? {}).map(([id, d]) => ({
    id,
    name: d.name,
    className: d.className,
    role: d.role,
    color: "#b9c7d8",
    pre: 0,
    post: 0,
  }));
  const rosterEntries: RosterEntry[] = Object.entries(documents.players ?? {})
    .map(([id, d]) => ({
      id,
      name: d.name,
      className: d.className ?? "Unknown",
      role: d.role ?? "Unknown",
      active: d.active ?? true,
      joinDate: d.joinDate ?? "",
      leaveDate: d.leaveDate ?? "",
      notes: d.notes ?? "",
      version: d.version ?? 0,
    }))
    .sort((a, b) => a.name.localeCompare(b.name));
  const characterEntries: CharacterEntry[] = Object.entries(
    documents.characters ?? {},
  )
    .map(([id, data]) => ({
      id,
      name: data.name,
      realm: data.realm,
      region: data.region,
      playerId: data.playerId,
      version: data.version,
    }))
    .sort(
      (a, b) =>
        a.name.localeCompare(b.name) ||
        a.realm.localeCompare(b.realm) ||
        a.region.localeCompare(b.region),
    );
  const publicPlayers = results.players as (Results["players"][number] &
    Player)[];
  const allPlayers = officer
    ? roster.map((p) => ({
        ...p,
        color: publicPlayers.find((r) => r.id === p.id)?.color ?? p.color,
      }))
    : publicPlayers.map((p) => ({ ...p, pre: 0, post: 0 }));
  return {
    versions,
    inputs,
    rosterEntries,
    characterEntries,
    allPlayers,
    calculation: {
      ...defaultCalculation(),
      ...dataOnly(documents.settings?.calculation ?? {}),
    } as CalculationSettings,
  };
}
export type WorkspaceData = ReturnType<typeof useWorkspaceData>;
