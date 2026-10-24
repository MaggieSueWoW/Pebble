import { useEffect, useRef, useState } from "react";
import type { User } from "firebase/auth";
import {
  collection,
  onSnapshot,
  query,
  where,
  type Timestamp,
} from "firebase/firestore";
import { db } from "../../data/firebase";
import { accessMessage, invitationLink, officerAction } from "./api";

type Officer = {
  id: string;
  enabled: boolean;
  name?: string;
  email?: string;
  grantedByName?: string;
  grantedAt?: Timestamp;
};
type Invite = {
  id: string;
  email: string;
  invitedByName: string;
  expiresAt: Timestamp;
  createdAt: Timestamp;
};
type Confirmation = {
  title: string;
  description: string;
  action: Record<string, string>;
};
const date = (value?: Timestamp) =>
  value ? value.toDate().toLocaleString() : "Administrator grant";

export function OfficersPage({ user }: { user: User }) {
  const [officers, setOfficers] = useState<Officer[] | null>(null);
  const [invitations, setInvitations] = useState<Invite[] | null>(null);
  const [email, setEmail] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [readError, setReadError] = useState("");
  const [attempt, setAttempt] = useState(0);
  const [message, setMessage] = useState("");
  const [link, setLink] = useState("");
  const [confirmation, setConfirmation] = useState<Confirmation | null>(null);
  const dialog = useRef<HTMLDialogElement>(null);
  const pending = useRef(false);
  useEffect(() => {
    const denied = () => {
      setOfficers(null);
      setInvitations(null);
      setReadError(
        "Couldn’t load officer access. Check your connection or retry.",
      );
    };
    const stopOfficers = onSnapshot(
      query(collection(db!, "officers"), where("enabled", "==", true)),
      (snapshot) => {
        setOfficers(
          snapshot.docs
            .map((doc) => ({ ...doc.data(), id: doc.id }) as Officer)
            .sort((a, b) =>
              (a.name || a.email || a.id).localeCompare(
                b.name || b.email || b.id,
              ),
            ),
        );
      },
      denied,
    );
    const stopInvites = onSnapshot(
      query(
        collection(db!, "officerInvitations"),
        where("status", "==", "pending"),
      ),
      (snapshot) => {
        setInvitations(
          snapshot.docs
            .map((doc) => ({ ...doc.data(), id: doc.id }) as Invite)
            .sort((a, b) => b.createdAt.toMillis() - a.createdAt.toMillis()),
        );
      },
      denied,
    );
    return () => {
      stopOfficers();
      stopInvites();
    };
  }, [attempt]);
  useEffect(() => {
    if (confirmation) dialog.current?.showModal();
    else dialog.current?.close();
  }, [confirmation]);
  async function perform() {
    if (!confirmation || pending.current) return;
    pending.current = true;
    setBusy(true);
    setError("");
    setMessage("");
    setLink("");
    try {
      const result = await officerAction<{ invitationId?: string }>(
        confirmation.action,
      );
      if (result.invitationId) {
        setLink(invitationLink(result.invitationId));
        setEmail("");
        setMessage(
          "Invitation ready. Copy the link and share it with the invited person.",
        );
      } else
        setMessage(
          confirmation.action.action === "cancel"
            ? "Invitation cancelled."
            : "Officer access removed.",
        );
      setConfirmation(null);
    } catch (reason) {
      setError(accessMessage(reason));
      setConfirmation(null);
    } finally {
      pending.current = false;
      setBusy(false);
    }
  }
  async function copy(id: string) {
    const value = invitationLink(id);
    setLink(value);
    try {
      await navigator.clipboard.writeText(value);
      setMessage("Invitation link copied.");
    } catch {
      setMessage("Select and copy the invitation link below.");
    }
  }
  const ready = officers !== null && invitations !== null && !readError;
  return (
    <section
      className="card panel-body officers-page"
      aria-label="Officer access management"
    >
      <p>
        Officers can edit inputs, run Pebble, and manage officer access. Access
        applies across seasons.
      </p>
      {readError && (
        <p role="alert">
          {readError}{" "}
          <button
            className="text-button"
            onClick={() => {
              setReadError("");
              setAttempt((n) => n + 1);
            }}
          >
            Retry officer list
          </button>
        </p>
      )}
      {error && (
        <p role="alert" className="field-error">
          {error}
        </p>
      )}
      <p role="status">{message}</p>
      {link && (
        <div className="field">
          <label htmlFor="officer-invitation-link">Invitation link</label>
          <input
            id="officer-invitation-link"
            readOnly
            value={link}
            onFocus={(e) => e.currentTarget.select()}
          />
        </div>
      )}
      <form
        className="officer-add"
        onSubmit={(event) => {
          event.preventDefault();
          setConfirmation({
            title: "Add officer?",
            description: `${email.trim()} will be able to edit inputs, run Pebble, and manage officer access after accepting. The invitation expires in seven days.`,
            action: { action: "invite", email: email.trim() },
          });
        }}
      >
        <div className="field">
          <label htmlFor="officer-email">Google account email</label>
          <input
            id="officer-email"
            type="email"
            maxLength={254}
            required
            value={email}
            disabled={!ready || busy}
            onChange={(e) => setEmail(e.target.value)}
          />
        </div>
        <button className="button primary" disabled={!ready || busy}>
          Add officer
        </button>
      </form>
      <h2>Current officers{officers ? ` (${officers.length})` : ""}</h2>
      {!ready && !readError && <p role="status">Loading officer access…</p>}
      {ready && (
        <div className="officer-table">
          <table role="table">
            <thead>
              <tr>
                <th scope="col">Officer</th>
                <th scope="col">Granted by / date</th>
                <th scope="col">Action</th>
              </tr>
            </thead>
            <tbody>
              {officers.map((person) => (
                <tr key={person.id}>
                  <td data-label="Officer">
                    <strong>
                      {person.name ||
                        (person.id === user.uid ? user.displayName : "") ||
                        person.email ||
                        "Officer"}
                    </strong>
                    {person.id === user.uid && (
                      <span className="badge">You</span>
                    )}
                    <br />
                    {person.email ||
                      (person.id === user.uid ? user.email : "") ||
                      "Identity unavailable — ask this officer to sign in"}
                  </td>
                  <td data-label="Granted by / date">
                    {person.grantedByName || "Administrator"}
                    <br />
                    {date(person.grantedAt)}
                  </td>
                  <td data-label="Action">
                    <button
                      className="text-button"
                      disabled={busy || officers.length <= 1}
                      title={
                        officers.length <= 1
                          ? "Add another officer before removing the last officer"
                          : undefined
                      }
                      onClick={() =>
                        setConfirmation({
                          title:
                            person.id === user.uid
                              ? "Leave officer access?"
                              : "Remove officer access?",
                          description: `${person.email || person.name || "This officer"} will lose officer access immediately. Their pending invitations will be cancelled.`,
                          action: { action: "remove", uid: person.id },
                        })
                      }
                    >
                      {person.id === user.uid ? "Leave" : "Remove access"}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {ready && officers.length === 1 && (
        <p>Add another officer before removing the last officer.</p>
      )}
      <h2>Pending invitations</h2>
      {ready && !invitations.length && <p>No pending invitations.</p>}
      {ready && invitations.length > 0 && (
        <div className="officer-table">
          <table role="table">
            <thead>
              <tr>
                <th scope="col">Google account</th>
                <th scope="col">Invited by / expires</th>
                <th scope="col">Actions</th>
              </tr>
            </thead>
            <tbody>
              {invitations.map((invite) => (
                <tr key={invite.id}>
                  <td data-label="Google account">{invite.email}</td>
                  <td data-label="Invited by / expires">
                    {invite.invitedByName}
                    <br />
                    {invite.expiresAt.toMillis() <= Date.now()
                      ? "Expired: "
                      : ""}
                    {date(invite.expiresAt)}
                  </td>
                  <td data-label="Actions">
                    <button
                      className="text-button"
                      disabled={
                        busy || invite.expiresAt.toMillis() <= Date.now()
                      }
                      onClick={() => void copy(invite.id)}
                    >
                      Copy invitation link
                    </button>{" "}
                    <button
                      className="text-button"
                      disabled={busy}
                      onClick={() =>
                        setConfirmation({
                          title: "Cancel invitation?",
                          description: `${invite.email} will no longer be able to accept this invitation.`,
                          action: { action: "cancel", invitationId: invite.id },
                        })
                      }
                    >
                      Cancel invitation
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <dialog
        ref={dialog}
        className="confirm-dialog"
        aria-labelledby="officer-confirm-title"
        aria-describedby="officer-confirm-description"
        onCancel={(e) => {
          e.preventDefault();
          if (!busy) setConfirmation(null);
        }}
      >
        <h2 id="officer-confirm-title">{confirmation?.title}</h2>
        <p id="officer-confirm-description">{confirmation?.description}</p>
        <div className="button-row">
          <button
            className="button secondary"
            disabled={busy}
            onClick={() => setConfirmation(null)}
          >
            Keep current access
          </button>
          <button
            className="button primary"
            disabled={busy}
            onClick={() => void perform()}
          >
            {busy ? "Saving…" : "Confirm"}
          </button>
        </div>
      </dialog>
    </section>
  );
}
