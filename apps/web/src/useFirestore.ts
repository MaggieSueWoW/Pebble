import { logOut, seasonLabel, signIn } from "./firebase";
import { usePublicResults } from "./usePublicResults";
import { inputCollections } from "./data/firestore/constants";
import { stableVersions } from "./data/firestore/documents";
import { usePrivateState } from "./data/firestore/usePrivateState";
import { useOfficerAccess } from "./data/firestore/useOfficerAccess";
import { usePrivateSubscriptions } from "./data/firestore/usePrivateSubscriptions";
import { useRunQueue } from "./data/firestore/useRunQueue";
import { useResultMetadata } from "./data/firestore/useResultMetadata";
import { useWorkspaceData } from "./data/firestore/useWorkspaceData";
import { useInputWrites } from "./data/firestore/useInputWrites";
import { useRosterWrites } from "./data/firestore/useRosterWrites";
import type { LiveConnection } from "./data/firestore/types";
export type { LiveConnection, Versions } from "./data/firestore/types";

/** Public facade: private data and operations share one authorization lifetime. */
export function useFirestore(): {
  connection: LiveConnection;
  loading: boolean;
} {
  const state = usePrivateState();
  const publicResults = usePublicResults();
  const { results } = publicResults;
  const resultRun = results.signature;
  const hasResults = Boolean(resultRun);
  const access = useOfficerAccess(state.clear);
  usePrivateSubscriptions(access, state);
  const queue = useRunQueue(access, state, results);
  useResultMetadata(access, state, resultRun);
  const data = useWorkspaceData(state, results, access.officer);
  const inputWrites = useInputWrites(access, state, data);
  const rosterWrites = useRosterWrites(access, state, data);
  const { officer } = access;
  const { consumed } = state;
  return {
    loading:
      !access.authReady ||
      !publicResults.ready ||
      (officer && state.loaded.length < inputCollections.length),
    connection: {
      saved: { inputs: data.inputs, results, history: queue.history },
      players: data.allPlayers,
      roster: data.rosterEntries,
      characters: data.characterEntries,
      calculation: data.calculation,
      ...inputWrites,
      ...rosterWrites,
      nights: results.nights.map((n) => n.night),
      user: access.user,
      officer,
      accessGeneration: access.accessGeneration,
      label: seasonLabel,
      online: state.now - state.heartbeat < 150000,
      active: queue.active,
      versions: data.versions,
      unprocessed:
        officer &&
        (consumed.run !== resultRun ||
          stableVersions(data.versions) !== stableVersions(consumed.versions)),
      error: state.error,
      accessError: access.accessError,
      retryAccess: access.retryAccess,
      busy: state.busy,
      hasResults,
      submitting: queue.submitting,
      queueReady: queue.queueReady,
      queueError: queue.queueError,
      retryQueue: queue.retryQueue,
      resultSources:
        officer && consumed.run === resultRun ? consumed.sources : [],
      resultDiagnostics:
        officer && consumed.run === resultRun ? consumed.diagnostics : {},
      resultsPending: publicResults.loading || queue.unseenSuccess,
      resultsError: publicResults.error,
      retryResults: publicResults.retry,
      requestRun: queue.requestRun,
      signIn,
      signOut: logOut,
    },
  };
}
