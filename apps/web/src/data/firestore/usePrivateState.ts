import { useCallback, useEffect, useRef, useState } from "react";
import type { DocumentData } from "firebase/firestore";
import type { RecordedNightDiagnostics } from "../../resultProvenance";
import type { Versions } from "./types";

/** Private buffers reset together before another officer session may use them. */
export function usePrivateState() {
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
  const [heartbeat, setHeartbeat] = useState(0);
  const [now, setNow] = useState(Date.now());
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [consumed, setConsumed] = useState<{
    run: string;
    versions: Versions;
    sources: { code: string; title: string; warningIndexes?: number[] }[];
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
  const clear = useCallback(() => {
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
  }, []);
  const fail = useCallback((e: { message: string }) => setError(e.message), []);
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 15000);
    return () => clearInterval(timer);
  }, []);
  return {
    documents,
    setDocuments,
    loaded,
    setLoaded,
    runs,
    setRuns,
    queue,
    setQueue,
    heartbeat,
    setHeartbeat,
    now,
    error,
    setError,
    busy,
    setBusy,
    consumed,
    setConsumed,
    submission,
    submitting,
    setSubmitting,
    submittedRun,
    setSubmittedRun,
    clear,
    fail,
  };
}
export type PrivateState = ReturnType<typeof usePrivateState>;
