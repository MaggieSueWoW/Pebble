import { useEffect } from "react";
import {
  collection,
  doc,
  limit,
  onSnapshot,
  orderBy,
  query,
} from "firebase/firestore";
import { db, seasonId } from "../firebase";
import { ACCESS_WAIT_MS, inputCollections } from "./constants";
import type { OfficerAccess } from "./useOfficerAccess";
import type { PrivateState } from "./usePrivateState";

export function usePrivateSubscriptions(
  access: OfficerAccess,
  state: PrivateState,
) {
  const {
    officer,
    user,
    authorized,
    session,
    accessGeneration,
    accessFailed,
    stopPrivate,
  } = access;
  const { setLoaded, setDocuments, setRuns, setHeartbeat } = state;
  useEffect(() => {
    if (!officer || !authorized.current) return;
    const own = session.current;
    let alive = true;
    const current = () =>
      alive && authorized.current && own === session.current;
    const pendingInputs = new Set(inputCollections);
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
    const subs = inputCollections.map((name) => {
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
  }, [
    officer,
    user,
    accessGeneration,
    accessFailed,
    authorized,
    session,
    setDocuments,
    setHeartbeat,
    setLoaded,
    setRuns,
    stopPrivate,
  ]);
}
