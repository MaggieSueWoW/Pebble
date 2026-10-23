import { useCallback, useEffect, useRef, useState } from "react";
import { onAuthStateChanged, type User } from "firebase/auth";
import { doc, onSnapshot } from "firebase/firestore";
import { auth, db } from "../../firebase";
import { ACCESS_ERROR, ACCESS_WAIT_MS } from "./constants";

/** Own authorization generations; invalidation precedes private-data cleanup. */
export function useOfficerAccess(clearData: () => void) {
  const [user, setUser] = useState<User | null>(null);
  const [officer, setOfficer] = useState(false);
  const [authReady, setAuthReady] = useState(false);
  const [authAttempt, setAuthAttempt] = useState(0);
  const [accessError, setAccessError] = useState("");
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
    clearData();
    setAccessError("");
  }, [clearData]);
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
  return {
    user,
    officer,
    authReady,
    accessError,
    accessGeneration,
    session,
    authorized,
    stopPrivate,
    stopConsumed,
    accessFailed,
    retryAccess,
    accessSession,
    assertAccess,
  };
}
export type OfficerAccess = ReturnType<typeof useOfficerAccess>;
