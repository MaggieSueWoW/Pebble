import { useCallback } from "react";
import {
  doc,
  runTransaction,
  serverTimestamp,
  type DocumentData,
} from "firebase/firestore";
import { db, seasonId } from "../firebase";
import { stableJSON, type Inputs } from "../../shared/model";
import {
  validateCalculation,
  type CalculationSettings,
} from "../../features/settings/calculation";
import { readLinkedPlayers, advanceLinkedPlayers } from "./linkedPlayers";
import { dataOnly } from "./documents";
import type { Versions } from "./types";
import type { OfficerAccess } from "./useOfficerAccess";
import type { PrivateState } from "./usePrivateState";
import type { WorkspaceData } from "./useWorkspaceData";

export function useInputWrites(
  access: OfficerAccess,
  state: PrivateState,
  data: WorkspaceData,
) {
  const { user, assertAccess, accessSession, session } = access;
  const { setBusy, setError, fail } = state;
  const { documents } = state;
  const { versions } = data;
  const saveInputs = useCallback(
    async (next: Inputs, expected = versions) => {
      assertAccess();
      const changes: { path: string; value: DocumentData }[] = [];
      for (const report of next.reports) {
        const old = documents.reports?.[report.code];
        const value = {
          enabled: report.enabled,
          notes: old?.notes ?? report.notes ?? "",
        };
        if (
          !old ||
          stableJSON(value) !==
            stableJSON({ enabled: old.enabled, notes: old.notes || "" })
        )
          changes.push({ path: `reports/${report.code}`, value });
      }
      for (const [key, a] of Object.entries(next.adjustments)) {
        const slash = key.indexOf("/"),
          nightId = key.slice(0, slash),
          playerId = key.slice(slash + 1);
        const path = `availabilityOverrides/${nightId}_${playerId}`;
        const old = documents.availabilityOverrides?.[`${nightId}_${playerId}`];
        const value = { nightId, playerId, ...a };
        if (!old || stableJSON(value) !== stableJSON(dataOnly(old)))
          changes.push({ path, value });
      }
      for (const [night, t] of Object.entries(next.timings)) {
        const old = documents.nightOverrides?.[night];
        if (!old || stableJSON(t) !== stableJSON(dataOnly(old)))
          changes.push({ path: `nightOverrides/${night}`, value: t });
      }
      if (!changes.length) return versions;
      if (changes.length > 100)
        throw new Error("Save at most 100 changes at a time.");
      setBusy(true);
      setError("");
      try {
        return await runTransaction(db!, async (transaction) => {
          assertAccess();
          const snapshots = await Promise.all(
            changes.map((c) =>
              transaction.get(doc(db!, `seasons/${seasonId}/${c.path}`)),
            ),
          );
          assertAccess();
          const linkedPlayers = await readLinkedPlayers(
            transaction,
            db!,
            seasonId,
            changes
              .filter((change) =>
                change.path.startsWith("availabilityOverrides/"),
              )
              .map((change) => change.value.playerId as string),
          );
          assertAccess();
          const acknowledged: Versions = { ...versions };
          changes.forEach((c, i) => {
            const actual = snapshots[i].data()?.version ?? 0;
            if (actual !== (expected[c.path] ?? 0))
              throw new Error(
                "Another officer changed this input. Cancel your edits to reload, then try again.",
              );
            acknowledged[c.path] = actual + 1;
            transaction.set(snapshots[i].ref, {
              ...c.value,
              version: actual + 1,
              updatedBy: user!.uid,
              updatedAt: serverTimestamp(),
            });
          });
          advanceLinkedPlayers(transaction, linkedPlayers, user!.uid);
          return acknowledged;
        });
      } catch (e) {
        if (accessSession === session.current) fail(e as Error);
        throw e;
      } finally {
        if (accessSession === session.current) setBusy(false);
      }
    },
    [documents, versions, user, assertAccess, accessSession, fail],
  );
  async function saveCalculation(value: CalculationSettings, expected: number) {
    assertAccess();
    const invalid = validateCalculation(value);
    if (invalid) throw new Error(invalid);
    setBusy(true);
    setError("");
    try {
      return await runTransaction(db!, async (transaction) => {
        assertAccess();
        const ref = doc(db!, "seasons", seasonId, "settings", "calculation");
        const snapshot = await transaction.get(ref);
        assertAccess();
        const actual = snapshot.data()?.version ?? 0;
        if (actual !== expected)
          throw new Error(
            "Another officer changed calculation settings. Cancel your edits to reload, then try again.",
          );
        transaction.set(ref, {
          ...value,
          version: actual + 1,
          updatedBy: user!.uid,
          updatedAt: serverTimestamp(),
        });
        return actual + 1;
      });
    } catch (e) {
      if (accessSession === session.current) fail(e as Error);
      throw e;
    } finally {
      if (accessSession === session.current) setBusy(false);
    }
  }
  return { saveInputs, saveCalculation };
}
