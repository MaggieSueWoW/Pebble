import { useEffect } from "react";
import { doc, onSnapshot, type DocumentData } from "firebase/firestore";
import { db, seasonId } from "../../firebase";
import type { OfficerAccess } from "./useOfficerAccess";
import type { PrivateState } from "./usePrivateState";

export function useResultMetadata(
  access: OfficerAccess,
  state: PrivateState,
  resultRun: string,
) {
  const {
    officer,
    user,
    authorized,
    session,
    accessGeneration,
    accessFailed,
    stopConsumed,
  } = access;
  const { setConsumed, fail } = state;
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
                ...(summary.warningIndexes
                  ? { warningIndexes: summary.warningIndexes as number[] }
                  : {}),
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
}
