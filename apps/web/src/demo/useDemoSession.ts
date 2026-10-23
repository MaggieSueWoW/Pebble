import { useEffect, useRef, useState } from "react";
import {
  calculate,
  freshState,
  signature,
  players,
  nightOptions,
  defaultTiming,
  timingSpans,
  demoCode,
} from "./model";
import type { Inputs, Saved } from "../model";
import type { Versions } from "../useFirestore";
const STORAGE = "pebble-prototype-v1";
function loadSaved(): Saved {
  try {
    const stored = JSON.parse(localStorage.getItem(STORAGE) || "null");
    if (
      stored?.schema === 1 &&
      Array.isArray(stored.data?.inputs?.reports) &&
      Array.isArray(stored.data?.results?.nights) &&
      Array.isArray(stored.data?.history)
    )
      return stored.data;
  } catch {
    /* Storage can be blocked or stale; the demo can still run in memory. */
  }
  return freshState();
}
type DemoRun = {
  id: string;
  status: "queued" | "running";
  stage: number;
  snapshot?: Inputs;
  fail?: boolean;
};

/** Browser-local persistence and the simulated worker exist only in sample mode. */
export function useDemoSession() {
  const [saved, setSaved] = useState(loadSaved);
  const [online, setOnline] = useState(true);
  const [failNext, setFailNext] = useState(false);
  const [active, setActive] = useState<DemoRun | null>(null);
  const [storageError, setStorageError] = useState(false);
  const [notification, setNotification] = useState<{
    text: string;
    error: boolean;
  } | null>(null);
  const inputRef = useRef(saved.inputs);
  const failRef = useRef(failNext);
  inputRef.current = saved.inputs;
  failRef.current = failNext;
  const notify = (text: string, error = false) =>
    setNotification({ text, error });
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE, JSON.stringify({ schema: 1, data: saved }));
      setStorageError(false);
    } catch {
      setStorageError(true);
    }
  }, [saved]);
  useEffect(() => {
    if (!active || active.status !== "queued" || !online) return;
    const timer = setTimeout(() => {
      setActive((current) =>
        current
          ? {
              ...current,
              status: "running",
              stage: 1,
              snapshot: structuredClone(inputRef.current),
              fail: failRef.current,
            }
          : null,
      );
      setFailNext(false);
    }, 650);
    return () => clearTimeout(timer);
  }, [active?.id, active?.status, online]);
  useEffect(() => {
    if (!active || active.status !== "running" || !active.snapshot) return;
    const current = active;
    const timers = [1400, 2600, 3800].map((time, i) =>
      setTimeout(
        () => setActive((r) => (r ? { ...r, stage: i + 2 } : null)),
        time,
      ),
    );
    timers.push(
      setTimeout(() => {
        const at = new Date().toISOString();
        try {
          if (current.fail)
            throw new Error(
              "The demo bot could not reach Warcraft Logs. Previous results are still available. Try running again.",
            );
          const nextResults = calculate(current.snapshot!, at);
          const nextInputs = {
            ...inputRef.current,
            reports: inputRef.current.reports.map((r) =>
              current.snapshot!.reports.some(
                (s) => s.code === r.code && s.enabled,
              )
                ? { ...r, fetched: true }
                : r,
            ),
          };
          // Fetched is UI metadata, so carry that update into the consumed snapshot too.
          const consumed = {
            ...current.snapshot!,
            reports: current.snapshot!.reports.map((r) =>
              r.enabled ? { ...r, fetched: true } : r,
            ),
          };
          nextResults.signature = signature(consumed);
          setSaved((old) => ({
            inputs: nextInputs,
            results: nextResults,
            history: [
              {
                id: current.id,
                at,
                status: "succeeded" as const,
                message: `${nextResults.reportCount} reports processed · ${nextResults.nights.length} raid nights`,
              },
              ...old.history,
            ].slice(0, 12),
          }));
          notify("Results updated. Your team would see this result set now.");
        } catch (e) {
          const message = (e as Error).message;
          setSaved((old) => ({
            ...old,
            history: [
              { id: current.id, at, status: "failed" as const, message },
              ...old.history,
            ].slice(0, 12),
          }));
          notify(message, true);
        }
        setActive(null);
      }, 4800),
    );
    return () => timers.forEach(clearTimeout);
  }, [active?.id, active?.status]);

  async function saveInputs(
    next: Inputs,
    expected: Versions = {},
  ): Promise<Versions> {
    setSaved((old) => ({
      ...old,
      inputs: {
        reports: [
          ...next.reports.filter(
            (r) =>
              !old.inputs.reports.some((existing) => existing.code === r.code),
          ),
          ...old.inputs.reports.map(
            (r) => next.reports.find((change) => change.code === r.code) ?? r,
          ),
        ],
        adjustments: { ...old.inputs.adjustments, ...next.adjustments },
        timings: { ...old.inputs.timings, ...next.timings },
      },
    }));
    return Object.fromEntries(
      Object.entries(expected).map(([path, version]) => [path, version + 1]),
    );
  }
  async function requestRun() {
    setActive({ id: crypto.randomUUID(), status: "queued", stage: 0 });
  }
  function reset() {
    setSaved(freshState());
    setFailNext(false);
    setOnline(true);
  }
  return {
    saved,
    players,
    nights: nightOptions,
    active,
    online,
    setOnline,
    failNext,
    setFailNext,
    storageError,
    notification,
    saveInputs,
    requestRun,
    reset,
    unprocessed: saved.results.signature !== signature(saved.inputs),
    detectedTiming: defaultTiming,
    timingSpans,
    sampleReportLink: `https://www.warcraftlogs.com/reports/${demoCode}`,
  };
}
export type DemoSession = ReturnType<typeof useDemoSession>;
