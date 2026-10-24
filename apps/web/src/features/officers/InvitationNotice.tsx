import { useEffect, useState } from "react";
import type { User } from "firebase/auth";
import { accessMessage, officerAction, type Session } from "./api";

/** Mounted within the auth/access keyed workspace: stale responses cannot cross accounts. */
export function InvitationNotice({
  user,
  officer,
  switchAccount,
  authPending,
}: {
  user: User;
  officer: boolean;
  switchAccount: () => void;
  authPending: boolean;
}) {
  const [session, setSession] = useState<Session | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [accepted, setAccepted] = useState(false);
  const [attempt, setAttempt] = useState(0);
  const [invitationId] = useState(
    () => new URLSearchParams(location.search).get("invite") || "",
  );
  useEffect(() => {
    let alive = true;
    officerAction<Session>({
      action: "session",
      ...(invitationId ? { invitationId } : {}),
    })
      .then((value) => {
        if (alive) {
          setSession(value);
          setError("");
        }
      })
      .catch((reason) => {
        if (alive) setError(accessMessage(reason));
      });
    return () => {
      alive = false;
    };
  }, [user.uid, invitationId, attempt]);
  async function accept(id: string) {
    setBusy(true);
    setError("");
    try {
      await officerAction({ action: "accept", invitationId: id });
      setAccepted(true);
      const url = new URL(location.href);
      url.searchParams.delete("invite");
      history.replaceState(history.state, "", url);
    } catch (reason) {
      setError(accessMessage(reason));
    } finally {
      setBusy(false);
    }
  }
  const linked = session?.invitation;
  // Membership can arrive before the acceptance RPC returns and remount us.
  // An already accepted link must not leave a stale invitation on that session.
  if (
    officer &&
    ((!invitationId && !error) ||
      (linked?.matchesAccount && linked.status === "accepted"))
  )
    return null;
  if (accepted)
    return (
      <section className="notice" role="status">
        Invitation accepted. Loading officer access…
      </section>
    );
  const invitations = linked ? [linked] : session?.invitations || [];
  if (!error && !invitations.length && !invitationId) return null;
  return (
    <section
      className="card panel-body invitation-notice"
      aria-label="Officer invitations"
    >
      <h2>Officer invitation</h2>
      {!session && !error && <p role="status">Checking invitation…</p>}
      {error && (
        <p role="alert">
          {error}{" "}
          <button
            className="text-button"
            disabled={busy}
            onClick={() => setAttempt((n) => n + 1)}
          >
            Retry invitation
          </button>
        </p>
      )}
      {invitations.map((invite) => (
        <div key={invite.id}>
          <p>
            {invite.invitedByName} invited <strong>{invite.email}</strong> to
            become an officer.
          </p>
          {invite.status !== "pending" ? (
            <p>
              This invitation is {invite.status}. Ask a current officer for a
              new invitation if you need access.
            </p>
          ) : invite.matchesAccount === false ? (
            <>
              <p>
                You are signed in as {user.email}. Switch to {invite.email} to
                accept.
              </p>
              <button
                className="button secondary"
                disabled={busy || authPending}
                onClick={switchAccount}
              >
                Switch Google account
              </button>
            </>
          ) : (
            <>
              <p>
                Officers can edit inputs, run Pebble, and manage officer access.
                Expires {new Date(invite.expiresAt).toLocaleString()}.
              </p>
              <button
                className="button primary"
                disabled={busy}
                onClick={() => void accept(invite.id)}
              >
                {busy ? "Accepting…" : "Accept officer access"}
              </button>
            </>
          )}
        </div>
      ))}
    </section>
  );
}
