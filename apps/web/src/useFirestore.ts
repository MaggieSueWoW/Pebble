import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { onAuthStateChanged, type User } from "firebase/auth";
import {
  collection,
  doc,
  getDocsFromServer,
  limit,
  onSnapshot,
  orderBy,
  query,
  runTransaction,
  serverTimestamp,
  where,
  type DocumentData,
} from "firebase/firestore";
import { planMainSwap } from "./mainSwap";
import { auth, db, logOut, seasonId, seasonLabel, signIn } from "./firebase";
import {
  adjustmentKey,
  stableJSON,
  type Inputs,
  type Player,
  type Results,
  type Saved,
} from "./model";
import {
  defaultCalculation,
  validateCalculation,
  type CalculationSettings,
} from "./calculation";
import { usePublicResults } from "./usePublicResults";

import { validatePlayer, type RosterEntry, type RosterValue } from "./roster";

import {
  normalizeCharacter,
  validateCharacter,
  type CharacterEntry,
  type CharacterValue,
} from "./characters";

import type { RecordedNightDiagnostics } from "./resultProvenance";

export type Versions = Record<string, number>;
export type LiveConnection = {
  saved: Saved;
  players: Player[];
  roster: RosterEntry[];
  characters: CharacterEntry[];
  saveCharacter: (
    id: string,
    value: CharacterValue,
    expected: number,
  ) => Promise<number>;
  calculation: CalculationSettings;
  saveCalculation: (
    value: CalculationSettings,
    expected: number,
  ) => Promise<number>;
  deletePlayer: (id: string, expected: Versions) => Promise<void>;
  deleteCharacter: (id: string, expected: number) => Promise<void>;
  swapMain: (
    playerId: string,
    altId: string,
    playerVersion: number,
    altVersion: number,
  ) => Promise<void>;
  savePlayer: (
    id: string,
    value: RosterValue,
    expected: number,
  ) => Promise<number>;
  nights: string[];
  user: User | null;
  officer: boolean;
  accessGeneration: number;
  label: string;
  online: boolean;
  active: { id: string; status: "queued" | "running"; stage: number } | null;
  versions: Versions;
  unprocessed: boolean;
  error: string;
  accessError: string;
  retryAccess: () => void;
  busy: boolean;
  hasResults: boolean;
  submitting: boolean;
  queueReady: boolean;
  queueError: string;
  retryQueue: () => void;
  resultSources: { code: string; title: string }[];
  resultDiagnostics: Record<string, RecordedNightDiagnostics>;
  resultsPending: boolean;
  resultsError: string;
  retryResults: () => void;
  saveInputs: (next: Inputs, expected?: Versions) => Promise<Versions>;
  requestRun: () => Promise<void>;
  signIn: () => Promise<void>;
  signOut: () => Promise<void>;
};
const ACCESS_WAIT_MS = 15000;
const ACCESS_ERROR =
  "Couldn’t verify officer access. Check your connection, then retry access or switch Google accounts.";
const names = [
  "reports",
  "players",
  "characters",
  "nightOverrides",
  "availabilityOverrides",
  "settings",
];
const stableVersions = (v: Versions) =>
  JSON.stringify(Object.entries(v).sort(([a], [b]) => a.localeCompare(b)));
const dataOnly = (d: DocumentData) =>
  Object.fromEntries(
    Object.entries(d).filter(
      ([k]) => !["version", "updatedBy", "updatedAt"].includes(k),
    ),
  );
const compareActiveRuns = (a: DocumentData, b: DocumentData) =>
  Number(b.status === "running") - Number(a.status === "running") ||
  time(a.requestedAt).localeCompare(time(b.requestedAt));
const time = (value: unknown) =>
  value && typeof (value as { toDate?: unknown }).toDate === "function"
    ? (value as { toDate: () => Date }).toDate().toISOString()
    : "";
export function useFirestore(): {
  connection: LiveConnection;
  loading: boolean;
} {
  const [user, setUser] = useState<User | null>(null);
  const [officer, setOfficer] = useState(false);
  const [authReady, setAuthReady] = useState(false);
  const [authAttempt, setAuthAttempt] = useState(0);
  const [accessError, setAccessError] = useState("");
  const [documents, setDocuments] = useState<
    Record<string, Record<string, DocumentData>>
  >({});
  const [loaded, setLoaded] = useState<string[]>([]);
  const [runs, setRuns] = useState<DocumentData[]>([]);
  const [queue, setQueue] = useState<{
    ready: boolean;
    error: string;
    runs: DocumentData[];
  }>({ ready: false, error: "", runs: [] });
  const [queueAttempt, setQueueAttempt] = useState(0);
  const publicResults = usePublicResults();
  const { results } = publicResults;
  const resultRun = results.signature;
  const hasResults = Boolean(resultRun);
  const [heartbeat, setHeartbeat] = useState(0);
  const [now, setNow] = useState(Date.now());
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [consumed, setConsumed] = useState<{
    run: string;
    versions: Versions;
    sources: { code: string; title: string }[];
    diagnostics: Record<string, RecordedNightDiagnostics>;
  }>({ run: "", versions: {}, sources: [], diagnostics: {} });
  const submission = useRef<{
    id: string;
    promise: Promise<void> | null;
    confirmed: boolean;
  } | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [submittedRun, setSubmittedRun] = useState<{
    id: string;
    data: DocumentData | null;
  } | null>(null);
  const [accessGeneration, setAccessGeneration] = useState(0);
  const [authGeneration, setAuthGeneration] = useState(0);
  const session = useRef(0);
  const authorized = useRef(false);
  const authSession = useRef(0);
  const stopConsumed = useRef<(() => void) | null>(null);
  const stopPrivate = useRef<(() => void) | null>(null);
  const clearPrivate = useCallback(() => {
    session.current += 1;
    authorized.current = false;
    stopPrivate.current?.();
    stopPrivate.current = null;
    stopConsumed.current?.();
    stopConsumed.current = null;
    setAccessGeneration(session.current);
    setOfficer(false);
    setDocuments({});
    setLoaded([]);
    setRuns([]);
    setQueue({ ready: false, error: "", runs: [] });
    setConsumed({ run: "", versions: {}, sources: [], diagnostics: {} });
    setHeartbeat(0);
    submission.current = null;
    setSubmitting(false);
    setSubmittedRun(null);
    setBusy(false);
    setError("");
    setAccessError("");
  }, []);
  const fail = useCallback((e: { message: string }) => setError(e.message), []);
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 15000);
    return () => clearInterval(timer);
  }, []);
  const accessFailed = useCallback(() => {
    clearPrivate();
    setAccessError(ACCESS_ERROR);
    setAuthReady(true);
  }, [clearPrivate]);
  const retryAccess = useCallback(() => {
    authSession.current += 1;
    clearPrivate();
    setAuthReady(false);
    setAuthGeneration(authSession.current);
    if (!user) setAuthAttempt((old) => old + 1);
  }, [user, clearPrivate]);
  useEffect(() => {
    let alive = true;
    const timeout = setTimeout(() => {
      if (!alive) return;
      accessFailed();
    }, ACCESS_WAIT_MS);
    const stop = onAuthStateChanged(
      auth!,
      (next) => {
        if (!alive) return;
        clearTimeout(timeout);
        authSession.current += 1;
        setAuthGeneration(authSession.current);
        clearPrivate();
        setUser(next);
        setAuthReady(!next);
      },
      () => {
        if (!alive) return;
        clearTimeout(timeout);
        authSession.current += 1;
        setAuthGeneration(authSession.current);
        setUser(null);
        accessFailed();
      },
    );
    return () => {
      alive = false;
      clearTimeout(timeout);
      stop();
      authSession.current += 1;
      session.current += 1;
      authorized.current = false;
      stopPrivate.current?.();
      stopConsumed.current?.();
    };
  }, [clearPrivate, accessFailed, authAttempt]);
  useEffect(() => {
    if (!user) return;
    let alive = true;
    const own = authSession.current;
    const timeout = setTimeout(() => {
      if (alive && own === authSession.current) accessFailed();
    }, ACCESS_WAIT_MS);
    // Re-grants belong to the same auth session, but load fresh private data.
    const stop = onSnapshot(
      doc(db!, "officers", user.uid),
      { includeMetadataChanges: true },
      (snapshot) => {
        if (!alive || own !== authSession.current) return;
        const enabled = snapshot.exists() && snapshot.data().enabled === true;
        // Cached grants do not establish current server authorization.
        if (snapshot.metadata.fromCache) {
          if (!enabled) clearPrivate();
          return;
        }
        clearTimeout(timeout);
        if (!enabled) {
          clearPrivate();
        } else {
          authorized.current = true;
          setAccessError("");
          setOfficer(true);
        }
        setAuthReady(true);
      },
      () => {
        if (!alive || own !== authSession.current) return;
        clearTimeout(timeout);
        accessFailed();
      },
    );
    return () => {
      alive = false;
      clearTimeout(timeout);
      stop();
    };
  }, [user, authGeneration, accessFailed, clearPrivate]);
  useEffect(() => {
    if (!officer || !authorized.current) return;
    const own = session.current;
    let alive = true;
    const current = () =>
      alive && authorized.current && own === session.current;
    const pendingInputs = new Set(names);
    const timeout = setTimeout(() => {
      if (current()) accessFailed();
    }, ACCESS_WAIT_MS);
    const inputLoaded = (name: string) => {
      pendingInputs.delete(name);
      if (!pendingInputs.size) clearTimeout(timeout);
      setLoaded((old) => (old.includes(name) ? old : [...old, name]));
    };
    const privateFail = () => {
      if (current()) accessFailed();
    };
    const subs = names.map((name) => {
      if (name === "settings")
        return onSnapshot(
          doc(db!, "seasons", seasonId, "settings", "calculation"),
          (snapshot) => {
            if (!current()) return;
            setDocuments((old) => ({
              ...old,
              settings: Object.fromEntries(
                snapshot.exists() ? [["calculation", snapshot.data()]] : [],
              ),
            }));
            inputLoaded(name);
          },
          privateFail,
        );
      return onSnapshot(
        collection(db!, "seasons", seasonId, name),
        (snapshot) => {
          if (!current()) return;
          setDocuments((old) => ({
            ...old,
            [name]: Object.fromEntries(
              snapshot.docs.map((d) => [d.id, d.data()]),
            ),
          }));
          inputLoaded(name);
        },
        privateFail,
      );
    });
    subs.push(
      onSnapshot(
        query(
          collection(db!, "seasons", seasonId, "runs"),
          orderBy("requestedAt", "desc"),
          limit(20),
        ),
        (snapshot) => {
          if (current())
            setRuns(
              snapshot.docs
                .filter((d) => !d.metadata?.hasPendingWrites)
                .map((d) => ({ ...d.data(), id: d.id })),
            );
        },
        privateFail,
      ),
    );
    subs.push(
      onSnapshot(
        doc(db!, "bot", "status"),
        (snapshot) => {
          if (current())
            setHeartbeat(snapshot.data()?.lastHeartbeatAt?.toMillis() || 0);
        },
        privateFail,
      ),
    );
    const stop = () => {
      alive = false;
      clearTimeout(timeout);
      subs.forEach((unsubscribe) => unsubscribe());
    };
    stopPrivate.current = stop;
    return () => {
      stop();
      if (stopPrivate.current === stop) stopPrivate.current = null;
    };
  }, [officer, user, accessGeneration, accessFailed]);
  useEffect(() => {
    if (!officer || !authorized.current) return;
    const own = session.current;
    let alive = true;
    const current = () =>
      alive && authorized.current && own === session.current;
    setQueue((old) => ({ ...old, ready: false, error: "" }));
    // One snapshot covers both statuses, so claiming a queued run cannot create
    // a false idle gap between separate queued and running listeners.
    const stop = onSnapshot(
      query(
        collection(db!, "seasons", seasonId, "runs"),
        where("status", "in", ["queued", "running"]),
      ),
      { includeMetadataChanges: true },
      (snapshot) => {
        if (!current() || snapshot.metadata.fromCache) return;
        const active = snapshot.docs
          .filter((d) => !d.metadata.hasPendingWrites)
          .map((d) => ({ ...d.data(), id: d.id }))
          .sort(compareActiveRuns);
        setQueue({ ready: true, error: "", runs: active });
        const first = active[0];
        // Reuse direct tracking to retain terminal details beyond recent history,
        // including after a reload or an uncertain submission acknowledgement.
        if (
          first &&
          (!submission.current || submission.current.id === first.id)
        ) {
          if (submission.current) submission.current.confirmed = true;
          setSubmittedRun({ id: first.id, data: first });
        }
      },
      (e) => {
        if (!current()) return;
        if (e.code === "permission-denied") {
          accessFailed();
          return;
        } else setQueue((old) => ({ ...old, ready: false, error: e.message }));
      },
    );
    return () => {
      alive = false;
      stop();
    };
  }, [officer, accessGeneration, queueAttempt, accessFailed]);
  useEffect(() => {
    if (!officer || !submittedRun || !authorized.current) return;
    const own = session.current;
    const id = submittedRun.id;
    let alive = true;
    const stop = onSnapshot(
      doc(db!, "seasons", seasonId, "runs", id),
      (snapshot) => {
        if (!alive || own !== session.current || !authorized.current) return;
        if (!snapshot.exists() || snapshot.metadata.hasPendingWrites) return;
        const data = snapshot.data();
        setSubmittedRun({ id, data });
        if (
          ["succeeded", "failed"].includes(data.status) &&
          submission.current?.id === id
        )
          submission.current = null;
      },
      (e) => {
        if (!alive || own !== session.current || !authorized.current) return;
        if (e.code === "permission-denied") {
          accessFailed();
          return;
        } else {
          // Keep the ID for a safe retry if this listener has terminated.
          if (submission.current?.id === id)
            submission.current.confirmed = false;
          setSubmittedRun(null);
        }
        fail(e);
      },
    );
    return () => {
      alive = false;
      stop();
    };
  }, [officer, accessGeneration, submittedRun?.id, fail, accessFailed]);
  useEffect(() => {
    setConsumed({ run: "", versions: {}, sources: [], diagnostics: {} });
    if (!officer || !resultRun || !authorized.current) return;
    const own = session.current;
    let alive = true;
    const current = () =>
      alive && authorized.current && own === session.current;
    const stop = onSnapshot(
      doc(db!, "seasons", seasonId, "runs", resultRun),
      (snapshot) => {
        if (current()) {
          const summaries: Record<string, DocumentData> =
            snapshot.data()?.reportSummaries ?? {};
          setConsumed({
            run: resultRun,
            versions: snapshot.data()?.inputVersions ?? {},
            diagnostics: snapshot.data()?.nightDiagnostics ?? {},
            sources: Object.entries(summaries)
              .map(([code, summary]) => ({
                code,
                title: summary.title?.trim() || code,
              }))
              .sort((a, b) => a.code.localeCompare(b.code)),
          });
        }
      },
      (e) => {
        if (!current()) return;
        if (e.code === "permission-denied") {
          accessFailed();
          return;
        }
        fail(e);
      },
    );
    const cleanup = () => {
      alive = false;
      stop();
    };
    stopConsumed.current = cleanup;
    return () => {
      cleanup();
      if (stopConsumed.current === cleanup) stopConsumed.current = null;
    };
  }, [officer, user, accessGeneration, resultRun, fail, accessFailed]);
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
          checkedAt: time(summaries[code]?.checkedAt),
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
  const accessSession = session.current;
  const assertAccess = useCallback(() => {
    if (
      !user ||
      !officer ||
      !authorized.current ||
      accessSession !== session.current
    )
      throw new Error(
        "Officer access changed. Sign in with an enabled officer account.",
      );
  }, [user, officer, accessSession]);
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
        throw new Error(
          "This main or its linked data changed. Review the latest details before deleting.",
        );
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
          throw new Error(
            "This main or its linked data changed. Review the latest details before deleting.",
          );
        refs.forEach((ref) => transaction.delete(ref));
      });
    } catch (cause) {
      if (accessSession === session.current) fail(cause as Error);
      throw cause;
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
  async function requestRun() {
    assertAccess();
    const prior = submission.current;
    if (prior?.promise) return prior.promise;
    if (prior?.confirmed) return;
    if (!queue.ready)
      throw new Error("Wait for the run queue to load, then try again.");
    if (queue.runs.length) return;
    const pending = prior ?? {
      id: crypto.randomUUID(),
      promise: null,
      confirmed: false,
    };
    submission.current = pending;
    setSubmitting(true);
    setBusy(true);
    setError("");
    const operation = (async () => {
      const ref = doc(db!, "seasons", seasonId, "runs", pending.id);
      await runTransaction(db!, async (transaction) => {
        assertAccess();
        const existing = await transaction.get(ref);
        assertAccess();
        if (!existing.exists())
          transaction.set(ref, {
            status: "queued",
            requestedBy: user!.uid,
            requestedAt: serverTimestamp(),
          });
      });
      assertAccess();
      pending.confirmed = true;
      setSubmittedRun((old) =>
        old?.id === pending.id ? old : { id: pending.id, data: null },
      );
    })();
    pending.promise = operation;
    try {
      await operation;
    } catch (e) {
      if (accessSession === session.current) fail(e as Error);
      throw e;
    } finally {
      pending.promise = null;
      if (accessSession === session.current) {
        setSubmitting(false);
        setBusy(false);
      }
    }
  }
  const visibleRuns = submittedRun?.data
    ? [
        { ...submittedRun.data, id: submittedRun.id },
        ...runs.filter((r) => r.id !== submittedRun.id),
      ].sort((a, b) => time(b.requestedAt).localeCompare(time(a.requestedAt)))
    : runs;
  const activeRun = [
    ...queue.runs,
    ...(submittedRun?.data &&
    ["queued", "running"].includes(submittedRun.data.status)
      ? [{ ...submittedRun.data, id: submittedRun.id }]
      : []),
  ].sort(compareActiveRuns)[0];
  const unseenSuccess = visibleRuns.some(
    (r) =>
      r.status === "succeeded" &&
      r.id !== resultRun &&
      (!hasResults || time(r.completedAt) > results.completedAt),
  );
  const stages = [
    "Queued",
    "Validating inputs",
    "Fetching reports",
    "Calculating results",
    "Updating results",
  ];
  const saved: Saved = {
    inputs,
    results,
    history: visibleRuns
      .filter((r) => r.status === "succeeded" || r.status === "failed")
      .map((r) => ({
        id: r.id,
        at: time(r.completedAt) || time(r.requestedAt),
        status: r.status,
        message:
          r.error ||
          (r.warnings?.length
            ? r.warnings.join(" · ")
            : "Computation completed"),
      })),
  };
  return {
    loading:
      !authReady ||
      !publicResults.ready ||
      (officer && loaded.length < names.length),
    connection: {
      saved,
      players: allPlayers,
      roster: rosterEntries,
      characters: characterEntries,
      saveCharacter,
      calculation: {
        ...defaultCalculation(),
        ...dataOnly(documents.settings?.calculation ?? {}),
      } as CalculationSettings,
      saveCalculation,
      savePlayer,
      deletePlayer,
      deleteCharacter,
      swapMain,
      nights: results.nights.map((n) => n.night),
      user,
      officer,
      accessGeneration,
      label: seasonLabel,
      online: now - heartbeat < 150000,
      active: activeRun
        ? {
            id: activeRun.id,
            status: activeRun.status,
            stage: Math.max(0, stages.indexOf(activeRun.stage || "Queued")),
          }
        : null,
      versions,
      unprocessed:
        officer &&
        (consumed.run !== resultRun ||
          stableVersions(versions) !== stableVersions(consumed.versions)),
      error,
      accessError,
      retryAccess,
      busy,
      hasResults,
      submitting: submitting || Boolean(submittedRun && !submittedRun.data),
      queueReady: queue.ready,
      queueError: queue.error,
      retryQueue: () => setQueueAttempt((old) => old + 1),
      resultSources:
        officer && consumed.run === resultRun ? consumed.sources : [],
      resultDiagnostics:
        officer && consumed.run === resultRun ? consumed.diagnostics : {},
      resultsPending: publicResults.loading || unseenSuccess,
      resultsError: publicResults.error,
      retryResults: publicResults.retry,
      saveInputs,
      requestRun,
      signIn,
      signOut: logOut,
    },
  };
}
