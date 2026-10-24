import { InvitationNotice } from "../features/officers/InvitationNotice";
import { Info } from "lucide-react";
import type { LiveConnection } from "../data/useFirestore";
import { Notice } from "../components/Feedback";
export function ConnectionNotices({
  connection,
  canManage,
  retryWithFocus,
  switchAccount,
  authPending,
  showResults = true,
}: {
  connection?: LiveConnection;
  canManage: boolean;
  switchAccount: () => void;
  authPending: boolean;
  showResults?: boolean;
  retryWithFocus: (retry: () => void) => void;
}) {
  return (
    <>
      {connection &&
        !connection.user &&
        new URLSearchParams(location.search).has("invite") && (
          <Notice>
            You've opened an officer invitation. Use Officer sign-in to continue
            with the invited Google account.
          </Notice>
        )}
      {connection?.user && (
        <InvitationNotice
          user={connection.user}
          officer={connection.officer}
          switchAccount={switchAccount}
          authPending={authPending}
        />
      )}
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
              This account does not have officer access. Ask a current officer
              to invite your Google account email. You can still read completed
              results. Editing inputs and running Pebble require an enabled
              officer account. Sign out and sign back in to use another account.
            </p>
            <details>
              <summary>Account details</summary>
              <p>
                Account ID: <code>{connection.user.uid}</code>
              </p>
              <p>An administrator can use this ID for account recovery.</p>
            </details>
          </div>
        </section>
      )}
      {showResults && connection?.resultsError && (
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
      {showResults &&
        connection &&
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
