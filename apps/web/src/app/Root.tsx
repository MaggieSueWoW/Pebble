import { useEffect, useRef } from "react";
import { Workspace } from "./Workspace";
import { configured } from "../data/firebase";
import { useFirestore } from "../data/useFirestore";

function Connected() {
  const { connection, loading } = useFirestore();
  const focusAfterAccess = useRef(false);
  useEffect(() => {
    if (!loading && focusAfterAccess.current) {
      if (document.activeElement === document.body)
        document.getElementById("page-title")?.focus({ preventScroll: true });
      focusAfterAccess.current = false;
    }
  }, [loading, connection.accessGeneration]);
  if (loading)
    return (
      <main className="setup-screen">
        <h1>Connecting to Pebble…</h1>
        <p>Loading the latest results and your officer access.</p>
        {connection.error && <p role="alert">{connection.error}</p>}
      </main>
    );
  return (
    <Workspace
      key={`${connection.user?.uid || "public"}:${connection.officer}:${connection.accessGeneration}`}
      connection={{
        ...connection,
        retryAccess: () => {
          focusAfterAccess.current = true;
          connection.retryAccess();
        },
      }}
    />
  );
}
export default function Root() {
  if (configured) return <Connected />;
  return (
    <main className="setup-screen">
      <img src="/pebble.svg" width="48" height="48" alt="" />
      <h1>Connect Pebble</h1>
      <p>
        Set the Firebase web configuration in <code>apps/web/.env.local</code>{" "}
        and restart the dev server. See the web app README for setup.
      </p>
    </main>
  );
}
