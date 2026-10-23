import { useEffect, useState } from "react";
import {
  collection,
  doc,
  onSnapshot,
  query,
  runTransaction,
  serverTimestamp,
  where,
} from "firebase/firestore";
import { db, seasonId } from "../firebase";
import type { Results } from "../../shared/model";
import { compareActiveRuns, timestampISO } from "./documents";
import type { OfficerAccess } from "./useOfficerAccess";
import type { PrivateState } from "./usePrivateState";

/** Keep uncertain acknowledgements and terminal tracking tied to the same request ID. */
export function useRunQueue(
  access: OfficerAccess,
  state: PrivateState,
  results: Results,
) {
  const {
    officer,
    authorized,
    session,
    accessGeneration,
    accessFailed,
    assertAccess,
    accessSession,
    user,
  } = access;
  const {
    queue,
    setQueue,
    submission,
    submittedRun,
    setSubmittedRun,
    setSubmitting,
    submitting,
    setBusy,
    setError,
    fail,
    runs,
  } = state;
  const [queueAttempt, setQueueAttempt] = useState(0);
  const resultRun = results.signature;
  const hasResults = Boolean(resultRun);
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
      ].sort((a, b) =>
        timestampISO(b.requestedAt).localeCompare(timestampISO(a.requestedAt)),
      )
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
      (!hasResults || timestampISO(r.completedAt) > results.completedAt),
  );
  const stages = [
    "Queued",
    "Validating inputs",
    "Fetching reports",
    "Calculating results",
    "Updating results",
  ];
  return {
    history: visibleRuns
      .filter((r) => r.status === "succeeded" || r.status === "failed")
      .map((r) => ({
        id: r.id,
        at: timestampISO(r.completedAt) || timestampISO(r.requestedAt),
        status: r.status as "succeeded" | "failed",
        message:
          r.error ||
          (r.warnings?.length
            ? r.warnings.join(" · ")
            : "Computation completed"),
      })),
    active: activeRun
      ? {
          id: activeRun.id,
          status: activeRun.status as "queued" | "running",
          stage: Math.max(0, stages.indexOf(activeRun.stage || "Queued")),
        }
      : null,
    unseenSuccess,
    submitting: submitting || Boolean(submittedRun && !submittedRun.data),
    queueReady: queue.ready,
    queueError: queue.error,
    retryQueue: () => setQueueAttempt((old) => old + 1),
    requestRun,
  };
}
