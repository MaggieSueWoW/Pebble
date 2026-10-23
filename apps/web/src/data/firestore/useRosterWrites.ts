import {
  doc,
  collection,
  query,
  where,
  getDocsFromServer,
  getDocFromServer,
  runTransaction,
  serverTimestamp,
} from "firebase/firestore";
import { db, seasonId } from "../../firebase";
import {
  validatePlayer,
  type RosterEntry,
  type RosterValue,
} from "../../roster";
import {
  normalizeCharacter,
  validateCharacter,
  type CharacterEntry,
  type CharacterValue,
} from "../../characters";
import { readLinkedPlayers, advanceLinkedPlayers } from "../../linkedPlayers";
import { planMainSwap } from "../../mainSwap";
import type { Versions } from "./types";
import type { OfficerAccess } from "./useOfficerAccess";
import type { PrivateState } from "./usePrivateState";
import type { WorkspaceData } from "./useWorkspaceData";

/** Main/alt transactions retain their shared conflict protocol and linked-player reads. */
export function useRosterWrites(
  access: OfficerAccess,
  state: PrivateState,
  data: WorkspaceData,
) {
  const { user, assertAccess, accessSession, session } = access;
  const { setBusy, setError, fail } = state;
  const { rosterEntries, characterEntries } = data;
  async function savePlayer(id: string, value: RosterValue, expected: number) {
    assertAccess();
    if (!/^[A-Za-z0-9_-]{1,100}$/.test(id))
      throw new Error("Invalid player ID.");
    const invalid = validatePlayer(value, rosterEntries, id);
    if (invalid) throw new Error(invalid);
    setBusy(true);
    setError("");
    try {
      return await runTransaction(db!, async (transaction) => {
        assertAccess();
        const ref = doc(db!, "seasons", seasonId, "players", id);
        const snapshot = await transaction.get(ref);
        assertAccess();
        const actual = snapshot.data()?.version ?? 0;
        if (actual !== expected)
          throw new Error(
            "Another officer changed this player. Cancel your edits to reload, then try again.",
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
  async function deletePlayer(id: string, expected: Versions) {
    assertAccess();
    if (!/^[A-Za-z0-9_-]{1,100}$/.test(id))
      throw new Error("Invalid player ID.");
    const conflict =
      "This main or its linked data changed. Review the latest details before deleting.";
    setBusy(true);
    setError("");
    try {
      const linked = await Promise.all(
        ["characters", "availabilityOverrides"].map(async (family) => {
          const snapshot = await getDocsFromServer(
            query(
              collection(db!, "seasons", seasonId, family),
              where("playerId", "==", id),
            ),
          );
          return snapshot.docs.map((entry) => ({
            path: `${family}/${entry.id}`,
            version: entry.data().version as number,
          }));
        }),
      );
      assertAccess();
      const dependencies = linked.flat();
      const paths = [
        `players/${id}`,
        ...dependencies.map((entry) => entry.path),
      ];
      if (
        paths.length !== Object.keys(expected).length ||
        paths.some((path) => !(path in expected)) ||
        dependencies.some((entry) => entry.version !== expected[entry.path])
      )
        throw new Error(conflict);
      // Every linked writer advances players/{id} atomically. Its expected version
      // therefore protects the enumerated dependency set, including new links.
      await runTransaction(db!, async (transaction) => {
        assertAccess();
        const refs = paths.map((path) =>
          doc(db!, `seasons/${seasonId}/${path}`),
        );
        const snapshots = await Promise.all(
          refs.map((ref) => transaction.get(ref)),
        );
        assertAccess();
        if (
          snapshots.some(
            (snapshot, index) =>
              !snapshot.exists() ||
              snapshot.data()?.version !== expected[paths[index]],
          )
        )
          throw new Error(conflict);
        refs.forEach((ref) => transaction.delete(ref));
      });
    } catch (cause) {
      let failure = cause;
      if (
        accessSession === session.current &&
        typeof cause === "object" &&
        cause !== null &&
        "code" in cause &&
        cause.code === "permission-denied"
      ) {
        // A concurrently removed/reassigned alt can fail delete rules before
        // Firestore reports the main's stale read. Keep the same review/retry UX.
        try {
          assertAccess();
          const main = await getDocFromServer(
            doc(db!, "seasons", seasonId, "players", id),
          );
          assertAccess();
          if (
            !main.exists() ||
            main.data()?.version !== expected[`players/${id}`]
          )
            failure = new Error(conflict);
        } catch {
          // Genuine permission denial/revocation retains its original error.
        }
      }
      if (accessSession === session.current) fail(failure as Error);
      throw failure;
    } finally {
      if (accessSession === session.current) setBusy(false);
    }
  }
  async function deleteCharacter(id: string, expected: number) {
    assertAccess();
    if (!/^[A-Za-z0-9_-]{1,100}$/.test(id)) throw new Error("Invalid alt ID.");
    setBusy(true);
    setError("");
    try {
      await runTransaction(db!, async (transaction) => {
        assertAccess();
        const ref = doc(db!, "seasons", seasonId, "characters", id);
        const snapshot = await transaction.get(ref);
        assertAccess();
        if (!snapshot.exists() || snapshot.data()?.version !== expected)
          throw new Error(
            "Another officer changed this alt. Review the latest details before deleting.",
          );
        const linkedPlayers = await readLinkedPlayers(
          transaction,
          db!,
          seasonId,
          [snapshot.data()!.playerId],
        );
        assertAccess();
        advanceLinkedPlayers(transaction, linkedPlayers, user!.uid);
        transaction.delete(ref);
      });
    } catch (cause) {
      if (accessSession === session.current) fail(cause as Error);
      throw cause;
    } finally {
      if (accessSession === session.current) setBusy(false);
    }
  }
  async function swapMain(
    playerId: string,
    altId: string,
    playerVersion: number,
    altVersion: number,
  ) {
    assertAccess();
    if (![playerId, altId].every((id) => /^[A-Za-z0-9_-]{1,100}$/.test(id)))
      throw new Error("Invalid main or alt ID.");
    const oldMainAltId = crypto.randomUUID();
    setBusy(true);
    setError("");
    try {
      await runTransaction(db!, async (transaction) => {
        assertAccess();
        const playerRef = doc(db!, "seasons", seasonId, "players", playerId);
        const altRef = doc(db!, "seasons", seasonId, "characters", altId);
        const [main, alt] = await Promise.all([
          transaction.get(playerRef),
          transaction.get(altRef),
        ]);
        assertAccess();
        if (
          !main.exists() ||
          !alt.exists() ||
          main.data()?.version !== playerVersion ||
          alt.data()?.version !== altVersion
        )
          throw new Error(
            "This main or alt changed. Review the latest details before swapping.",
          );
        const plan = planMainSwap(
          { id: playerId, ...main.data() } as RosterEntry,
          { id: altId, ...alt.data() } as CharacterEntry,
          rosterEntries,
          characterEntries,
        );
        if (plan.reused) {
          const existing = await transaction.get(
            doc(db!, "seasons", seasonId, "characters", plan.reused.id),
          );
          assertAccess();
          if (
            !existing.exists() ||
            existing.data()?.version !== plan.reused.version
          )
            throw new Error(
              "The old main's alt mapping changed. Review and try again.",
            );
        }
        const metadata = { updatedBy: user!.uid, updatedAt: serverTimestamp() };
        transaction.set(playerRef, {
          ...plan.player,
          version: playerVersion + 1,
          ...metadata,
        });
        if (!plan.reused)
          transaction.set(
            doc(db!, "seasons", seasonId, "characters", oldMainAltId),
            { ...plan.replacement, version: 1, ...metadata },
          );
      });
    } catch (cause) {
      if (accessSession === session.current) fail(cause as Error);
      throw cause;
    } finally {
      if (accessSession === session.current) setBusy(false);
    }
  }
  async function saveCharacter(
    id: string,
    raw: CharacterValue,
    expected: number,
  ) {
    assertAccess();
    const value = normalizeCharacter(raw);
    if (!/^[A-Za-z0-9_-]{1,100}$/.test(id))
      throw new Error("Invalid character mapping ID.");
    const invalid = validateCharacter(
      value,
      characterEntries,
      rosterEntries,
      id,
    );
    if (invalid) throw new Error(invalid);
    setBusy(true);
    setError("");
    try {
      return await runTransaction(db!, async (transaction) => {
        assertAccess();
        const ref = doc(db!, "seasons", seasonId, "characters", id);
        const snapshot = await transaction.get(ref);
        assertAccess();
        const actual = snapshot.data()?.version ?? 0;
        if (actual !== expected)
          throw new Error(
            "Another officer changed this mapping. Cancel your edits to reload, then try again.",
          );
        const linkedPlayers = await readLinkedPlayers(
          transaction,
          db!,
          seasonId,
          [
            value.playerId,
            ...(snapshot.exists() ? [snapshot.data()!.playerId as string] : []),
          ],
        );
        assertAccess();
        advanceLinkedPlayers(transaction, linkedPlayers, user!.uid);
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
  return { savePlayer, deletePlayer, deleteCharacter, swapMain, saveCharacter };
}
