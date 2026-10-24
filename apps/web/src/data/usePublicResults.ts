import { useCallback, useEffect, useState } from "react";
import {
  collection,
  doc,
  getDocFromServer,
  getDocsFromServer,
  onSnapshot,
} from "firebase/firestore";
import { db, seasonId } from "./firebase";
import type { Results } from "../shared/model";
import { readWeekResults } from "../features/results/weeklyResults";

const emptyResults = (): Results => ({
  completedAt: "",
  signature: "",
  nights: [],
  weeks: [],
  players: [],
  reportCount: 0,
  warnings: [],
});

/** Load each public result set together, retaining the previous view on failure. */
export function usePublicResults() {
  const [results, setResults] = useState<Results>(emptyResults);
  const [ready, setReady] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [attempt, setAttempt] = useState(0);
  const retry = useCallback(() => {
    setLoading(true);
    setError("");
    setAttempt((old) => old + 1);
  }, []);

  useEffect(() => {
    let generation = 0;
    let alive = true;
    const stop = onSnapshot(
      doc(db!, "publicSeasons", seasonId),
      async (snapshot) => {
        if (!alive) return;
        const own = ++generation;
        const current = () => alive && own === generation;
        const id = snapshot.data()?.latestRunId;
        setLoading(true);
        setError("");
        if (!id) {
          setResults(emptyResults());
          setReady(true);
          setLoading(false);
          return;
        }
        try {
          const base = doc(db!, "seasons", seasonId, "results", id);
          const [root, playerDocs, nightDocs, weekDocs] = await Promise.all([
            getDocFromServer(base),
            getDocsFromServer(collection(base, "players")),
            getDocsFromServer(collection(base, "nights")),
            getDocsFromServer(collection(base, "weeks")),
          ]);
          if (!current()) return;
          if (!root.exists() || root.data().complete !== true)
            throw new Error("Incomplete result set");
          const completedAt = root.data().completedAt?.toDate().toISOString();
          if (!completedAt) throw new Error("Missing completion time");
          const players = playerDocs.docs.map((d) =>
            d.data(),
          ) as Results["players"];
          const nights = nightDocs.docs.map((d) =>
            d.data(),
          ) as Results["nights"];
          setResults({
            completedAt,
            signature: id,
            reportCount: root.data().reportCount,
            warnings: root.data().warnings ?? [],
            players,
            nights: nights.sort((a, b) => b.night.localeCompare(a.night)),
            weeks: readWeekResults(
              weekDocs.docs.map((d) => ({
                week: d.id,
                players: d.data().players,
              })),
              [...players, ...nights.flatMap((n) => n.players)],
            ),
          });
          setReady(true);
          setLoading(false);
        } catch {
          if (!current()) return;
          setError("Couldn’t load the latest results. Try again.");
          setReady(true);
          setLoading(false);
        }
      },
      () => {
        if (!alive) return;
        // A read already in flight must not clear a subscription failure.
        generation += 1;
        setError("Couldn’t connect to results. Try again.");
        setReady(true);
        setLoading(false);
      },
    );
    return () => {
      alive = false;
      generation += 1;
      stop();
    };
  }, [attempt]);

  return { results, ready, loading, error, retry };
}
