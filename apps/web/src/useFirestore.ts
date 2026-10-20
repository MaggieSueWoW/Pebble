import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { onAuthStateChanged, type User } from "firebase/auth";
import {
  collection,
  doc,
  limit,
  onSnapshot,
  orderBy,
  query,
  runTransaction,
  serverTimestamp,
  where,
  type DocumentData,
} from "firebase/firestore";
import { auth, db, logOut, seasonId, seasonLabel, signIn } from "./firebase";
import {
  adjustmentKey,
  stableJSON,
  type Inputs,
  type Player,
  type Results,
  type Saved,
} from "./model";
import { usePublicResults } from "./usePublicResults";

export type Versions = Record<string, number>;
export type LiveConnection = {
  saved: Saved;
  players: Player[];
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
  busy: boolean;
  hasResults: boolean;
  submitting: boolean;
  queueReady: boolean;
  queueError: string;
  retryQueue: () => void;
  resultsPending: boolean;
  resultsError: string;
  retryResults: () => void;
  saveInputs: (next: Inputs, expected?: Versions) => Promise<Versions>;
  requestRun: () => Promise<void>;
  signIn: () => Promise<void>;
  signOut: () => Promise<void>;
};
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
  const [consumed, setConsumed] = useState<{ run: string; versions: Versions }>(
    { run: "", versions: {} },
  );
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
    setConsumed({ run: "", versions: {} });
    setHeartbeat(0);
    submission.current = null;
    setSubmitting(false);
    setSubmittedRun(null);
    setBusy(false);
    setError("");
  }, []);
  const fail = useCallback((e: { message: string }) => setError(e.message), []);
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 15000);
    return () => clearInterval(timer);
  }, []);
  useEffect(() => {
    const stop = onAuthStateChanged(auth!, (next) => {
      authSession.current += 1;
      setAuthGeneration(authSession.current);
      clearPrivate();
      setUser(next);
      setAuthReady(!next);
    });
    return () => {
      stop();
      authSession.current += 1;
      session.current += 1;
      authorized.current = false;
      stopPrivate.current?.();
      stopConsumed.current?.();
    };
  }, [clearPrivate]);
  useEffect(() => {
    if (!user) return;
    let alive = true;
    const own = authSession.current;
    // Re-grants belong to the same auth session, but load fresh private data.
    const stop = onSnapshot(
      doc(db!, "officers", user.uid),
      (snapshot) => {
        if (!alive || own !== authSession.current) return;
        const enabled = snapshot.exists() && snapshot.data().enabled === true;
        if (!enabled) {
          clearPrivate();
        } else {
          authorized.current = true;
          setOfficer(true);
        }
        setAuthReady(true);
      },
      (e) => {
        if (!alive || own !== authSession.current) return;
        clearPrivate();
        fail(e);
        setAuthReady(true);
      },
    );
    return () => {
      alive = false;
      stop();
    };
  }, [user, authGeneration, fail, clearPrivate]);
  useEffect(() => {
    if (!officer || !authorized.current) return;
    const own = session.current;
    let alive = true;
    const current = () =>
      alive && authorized.current && own === session.current;
    const privateFail = (e: { message: string; code?: string }) => {
      if (!current()) return;
      if (e.code === "permission-denied") clearPrivate();
      fail(e);
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
            setLoaded((old) => (old.includes(name) ? old : [...old, name]));
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
          setLoaded((old) => (old.includes(name) ? old : [...old, name]));
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
      subs.forEach((unsubscribe) => unsubscribe());
    };
    stopPrivate.current = stop;
    return () => {
      stop();
      if (stopPrivate.current === stop) stopPrivate.current = null;
    };
  }, [officer, user, accessGeneration, fail, clearPrivate]);
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
        if (e.code === "permission-denied") clearPrivate();
        else setQueue((old) => ({ ...old, ready: false, error: e.message }));
      },
    );
    return () => {
      alive = false;
      stop();
    };
  }, [officer, accessGeneration, queueAttempt, clearPrivate]);
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
        if (e.code === "permission-denied") clearPrivate();
        else {
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
  }, [officer, accessGeneration, submittedRun?.id, fail, clearPrivate]);
  useEffect(() => {
    setConsumed({ run: "", versions: {} });
    if (!officer || !resultRun || !authorized.current) return;
    const own = session.current;
    let alive = true;
    const current = () =>
      alive && authorized.current && own === session.current;
    const stop = onSnapshot(
      doc(db!, "seasons", seasonId, "runs", resultRun),
      (snapshot) => {
        if (current())
          setConsumed({
            run: resultRun,
            versions: snapshot.data()?.inputVersions ?? {},
          });
      },
      (e) => {
        if (!current()) return;
        if (e.code === "permission-denied") clearPrivate();
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
  }, [officer, user, accessGeneration, resultRun, fail, clearPrivate]);
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
      busy,
      hasResults,
      submitting: submitting || Boolean(submittedRun && !submittedRun.data),
      queueReady: queue.ready,
      queueError: queue.error,
      retryQueue: () => setQueueAttempt((old) => old + 1),
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
