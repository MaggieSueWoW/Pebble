import { useEffect, useRef, type ReactNode } from "react";
import { Info } from "lucide-react";
export function Badge({
  children,
  tone = "neutral",
}: {
  children: ReactNode;
  tone?: "neutral" | "green" | "amber" | "purple" | "red";
}) {
  return <span className={`badge ${tone}`}>{children}</span>;
}
export function Notice({
  children,
  tone = "neutral",
}: {
  children: ReactNode;
  tone?: string;
}) {
  return (
    <div className={`notice ${tone}`}>
      <Info size={16} />
      <span>{children}</span>
    </div>
  );
}
export function ConfirmDiscard({
  action,
  cancel,
  discard,
}: {
  action: (() => void) | null;
  cancel: () => void;
  discard: () => void;
}) {
  const ref = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    if (action) ref.current?.showModal();
    else ref.current?.close();
  }, [action]);
  return (
    <dialog
      ref={ref}
      className="confirm-dialog"
      aria-labelledby="discard-title"
      aria-describedby="discard-description"
      onCancel={(e) => {
        e.preventDefault();
        cancel();
      }}
    >
      <h2 id="discard-title">Keep your changes?</h2>
      <p id="discard-description">
        You have unsaved edits. Save them before leaving, or discard them to
        continue.
      </p>
      <div className="button-row">
        <button className="button secondary" onClick={cancel}>
          Keep editing
        </button>
        <button className="button primary" onClick={discard}>
          Discard edits
        </button>
      </div>
    </dialog>
  );
}
