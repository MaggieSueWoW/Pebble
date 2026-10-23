import { Info } from "lucide-react";
import type { LiveConnection } from "../data/useFirestore";
import { Notice } from "../components/Feedback";
export function ConnectionNotices({
  connection,
  canManage,
  retryWithFocus,
}: {
  connection?: LiveConnection;
  canManage: boolean;
  retryWithFocus: (retry: () => void) => void;
}) {
  return (
    <>
      {connection?.error && <Notice tone="amber">{connection.error}</Notice>}
      {connection?.accessError && (
        <Notice tone="amber">
          <span role="alert">{connection.accessError}</span>{" "}
          <button className="text-button" onClick={connection.retryAccess}>
            Retry officer access
          </button>
        </Notice>
      )}
      {connection?.user && !connection.officer && !connection.accessError && (
        <section className="notice" aria-labelledby="officer-access-title">
          <Info size={16} aria-hidden="true" />
          <div>
            <h2 id="officer-access-title">Officer access not enabled</h2>
            <p>
              This account does not have officer access. You can still read
              completed results. Editing inputs and running Pebble require an
              enabled officer account. Sign out and sign back in to use another
              account.
            </p>
            <details>
              <summary>Account details</summary>
              <p>
                Account ID: <code>{connection.user.uid}</code>
              </p>
              <p>Use this ID when enabling officer access.</p>
            </details>
          </div>
        </section>
      )}
      {connection?.resultsError && (
        <Notice tone="amber">
          <span role="alert">{connection.resultsError}</span>{" "}
          <button
            className="text-button"
            onClick={() => retryWithFocus(connection.retryResults)}
          >
            Retry loading results
          </button>
        </Notice>
      )}
      {connection &&
        !connection.hasResults &&
        !connection.resultsPending &&
        !connection.resultsError && (
          <Notice>
            No completed results yet.{" "}
            {canManage
              ? "Add or include reports, then run Pebble to create the first result set."
              : "Completed results will appear here after an officer runs Pebble."}
          </Notice>
        )}
    </>
  );
}
