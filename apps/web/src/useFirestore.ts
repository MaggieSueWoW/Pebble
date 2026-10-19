import { useCallback, useEffect, useMemo, useState } from "react";
import { onAuthStateChanged, type User } from "firebase/auth";
import {
  collection,
  doc,
  getDoc,
  getDocs,
  limit,
  onSnapshot,
  orderBy,
  query,
  runTransaction,
  serverTimestamp,
  setDoc,
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

export type Versions = Record<string, number>;
export type LiveConnection = {
  saved: Saved;
  players: Player[];
  nights: string[];
  user: User | null;
  officer: boolean;
  label: string;
  online: boolean;
  active: { id: string; status: "queued" | "running"; stage: number } | null;
  versions: Versions;
  unprocessed: boolean;
  error: string;
  busy: boolean;
  hasResults: boolean;
  saveInputs: (next: Inputs, expected?: Versions) => Promise<void>;
  requestRun: () => Promise<void>;
  signIn: () => Promise<void>;
  signOut: () => Promise<void>;
};
const emptyResults = (): Results => ({
  completedAt: new Date().toISOString(),
  signature: "",
  nights: [],
  players: [],
  reportCount: 0,
});
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
  const [results, setResults] = useState<Results>(emptyResults);
  const [hasResults, setHasResults] = useState(false);
  const [publicReady, setPublicReady] = useState(false);
  const [resultRun, setResultRun] = useState("");
  const [heartbeat, setHeartbeat] = useState(0);
  const [now, setNow] = useState(Date.now());
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [consumed, setConsumed] = useState<Versions>({});
  const fail = useCallback((e: { message: string }) => setError(e.message), []);
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 15000);
    return () => clearInterval(timer);
  }, []);
  useEffect(
    () =>
      onAuthStateChanged(auth!, (next) => {
        setUser(next);
        setOfficer(false);
        setAuthReady(!next);
        setDocuments({});
        setLoaded([]);
        setRuns([]);
        setConsumed({});
        setError("");
      }),
    [],
  );
  useEffect(() => {
    if (!user) return;
    return onSnapshot(
      doc(db!, "officers", user.uid),
      (snapshot) => {
        setOfficer(snapshot.exists() && snapshot.data().enabled === true);
        setAuthReady(true);
      },
      (e) => {
        fail(e);
        setAuthReady(true);
      },
    );
  }, [user, fail]);
  useEffect(() => {
    if (!officer) return;
    const subs = names.map((name) => {
      if (name === "settings")
        return onSnapshot(
          doc(db!, "seasons", seasonId, "settings", "calculation"),
          (snapshot) => {
            setDocuments((old) => ({
              ...old,
              settings: Object.fromEntries(
                snapshot.exists() ? [["calculation", snapshot.data()]] : [],
              ),
            }));
            setLoaded((old) => (old.includes(name) ? old : [...old, name]));
          },
          fail,
        );
      return onSnapshot(
        collection(db!, "seasons", seasonId, name),
        (snapshot) => {
          setDocuments((old) => ({
            ...old,
            [name]: Object.fromEntries(
              snapshot.docs.map((d) => [d.id, d.data()]),
            ),
          }));
          setLoaded((old) => (old.includes(name) ? old : [...old, name]));
        },
        fail,
      );
    });
    subs.push(
      onSnapshot(
        query(
          collection(db!, "seasons", seasonId, "runs"),
          orderBy("requestedAt", "desc"),
          limit(20),
        ),
        (snapshot) =>
          setRuns(snapshot.docs.map((d) => ({ ...d.data(), id: d.id }))),
        fail,
      ),
    );
    subs.push(
      onSnapshot(
        doc(db!, "bot", "status"),
        (snapshot) =>
          setHeartbeat(snapshot.data()?.lastHeartbeatAt?.toMillis() || 0),
        fail,
      ),
    );
    return () => subs.forEach((stop) => stop());
  }, [officer, fail]);
  useEffect(() => {
    let generation = 0;
    return onSnapshot(
      doc(db!, "publicSeasons", seasonId),
      async (snapshot) => {
        const own = ++generation;
        const id = snapshot.data()?.latestRunId;
        if (!id) {
          setResults(emptyResults());
          setHasResults(false);
          setResultRun("");
          setPublicReady(true);
          return;
        }
        try {
          const base = doc(db!, "seasons", seasonId, "results", id);
          const [root, playerDocs, nightDocs] = await Promise.all([
            getDoc(base),
            getDocs(collection(base, "players")),
            getDocs(collection(base, "nights")),
          ]);
          if (own !== generation) return;
          if (!root.exists() || root.data().complete !== true)
            throw new Error("Latest results are not complete.");
          setResults({
            completedAt: time(root.data().completedAt),
            signature: id,
            reportCount: root.data().reportCount,
            players: playerDocs.docs.map((d) => d.data()) as Results["players"],
            nights: nightDocs.docs
              .map((d) => d.data())
              .sort((a, b) =>
                b.night.localeCompare(a.night),
              ) as Results["nights"],
          });
          setResultRun(id);
          setHasResults(true);
          setPublicReady(true);
        } catch (e) {
          if (own === generation) {
            fail(e as Error);
            setPublicReady(true);
          }
        }
      },
      (e) => {
        fail(e);
        setPublicReady(true);
      },
    );
  }, [fail]);
  useEffect(() => {
    setConsumed({});
    if (!officer || !resultRun) return;
    return onSnapshot(
      doc(db!, "seasons", seasonId, "runs", resultRun),
      (snapshot) => setConsumed(snapshot.data()?.inputVersions ?? {}),
      fail,
    );
  }, [officer, resultRun, fail]);
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
  const allPlayers = roster.length
    ? roster.map((p) => ({
        ...p,
        color: publicPlayers.find((r) => r.id === p.id)?.color ?? p.color,
      }))
    : publicPlayers.map((p) => ({ ...p, pre: 0, post: 0 }));
  const saveInputs = useCallback(
    async (next: Inputs, expected = versions) => {
      if (!user || !officer) throw new Error("Officer sign-in required.");
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
      if (!changes.length) return;
      if (changes.length > 100)
        throw new Error("Save at most 100 changes at a time.");
      setBusy(true);
      setError("");
      try {
        await runTransaction(db!, async (transaction) => {
          const snapshots = await Promise.all(
            changes.map((c) =>
              transaction.get(doc(db!, `seasons/${seasonId}/${c.path}`)),
            ),
          );
          changes.forEach((c, i) => {
            const actual = snapshots[i].data()?.version ?? 0;
            if (actual !== (expected[c.path] ?? 0))
              throw new Error(
                "Another officer changed this input. Cancel your edits to reload, then try again.",
              );
            transaction.set(snapshots[i].ref, {
              ...c.value,
              version: actual + 1,
              updatedBy: user.uid,
              updatedAt: serverTimestamp(),
            });
          });
        });
      } catch (e) {
        fail(e as Error);
        throw e;
      } finally {
        setBusy(false);
      }
    },
    [documents, versions, user, officer, fail],
  );
  const [pendingId, setPendingId] = useState<string | null>(null);
  async function requestRun() {
    if (!user || !officer) throw new Error("Officer sign-in required.");
    setBusy(true);
    setError("");
    const id = pendingId || crypto.randomUUID();
    setPendingId(id);
    try {
      const ref = doc(db!, "seasons", seasonId, "runs", id);
      const existing = await getDoc(ref);
      if (!existing.exists())
        await setDoc(ref, {
          status: "queued",
          requestedBy: user.uid,
          requestedAt: serverTimestamp(),
        });
      setPendingId(null);
    } catch (e) {
      fail(e as Error);
      throw e;
    } finally {
      setBusy(false);
    }
  }
  const activeRun = [...runs]
    .reverse()
    .find((r) => ["queued", "running"].includes(r.status));
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
    history: runs
      .filter((r) => r.status === "succeeded" || r.status === "failed")
      .map((r) => ({
        id: r.id,
        at: time(r.completedAt) || time(r.requestedAt),
        status: r.status,
        message:
          r.error ||
          (r.warnings?.length ? r.warnings.join(" · ") : "Results updated"),
      })),
  };
  return {
    loading:
      !authReady || !publicReady || (officer && loaded.length < names.length),
    connection: {
      saved,
      players: allPlayers,
      nights: results.nights.map((n) => n.night),
      user,
      officer,
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
        officer && stableVersions(versions) !== stableVersions(consumed),
      error,
      busy,
      hasResults,
      saveInputs,
      requestRun,
      signIn,
      signOut: logOut,
    },
  };
}
