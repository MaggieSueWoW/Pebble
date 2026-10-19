import { useEffect, useRef, useState, type SetStateAction } from "react";
import {
  emptyAdjustment,
  stableJSON,
  validateAdjustment,
  type Adjustment,
} from "./model";

type Draft = {
  epoch: number;
  value: Adjustment;
  base: Adjustment;
  version: number;
};

/** Bind drafts and save acknowledgements to one player/night and version. */
export function useAvailabilityEditor(
  key: string,
  saved: Adjustment,
  version: number,
  bounds: { pre: number; post: number },
  saveValue: (
    key: string,
    value: Adjustment,
    expected: number,
  ) => Promise<number>,
  busy = false,
  enabled = true,
) {
  const context = useRef({ key, epoch: 0 });
  if (context.current.key !== key)
    context.current = { key, epoch: context.current.epoch + 1 };
  const epoch = context.current.epoch;
  const [draft, setDraft] = useState<Draft | null>(null);
  const draftRef = useRef(draft);
  const pending = useRef(false);
  const alive = useRef(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const current = draft?.epoch === epoch ? draft : null;
  const value = current?.value ?? saved;
  const dirty = Boolean(
    current && stableJSON(current.value) !== stableJSON(current.base),
  );
  const validation = validateAdjustment(value, bounds.pre, bounds.post);
  function store(next: Draft | null) {
    draftRef.current = next;
    setDraft(next);
  }
  useEffect(() => {
    alive.current = true;
    return () => {
      alive.current = false;
    };
  }, []);
  useEffect(() => {
    setError("");
    setMessage("");
  }, [epoch]);
  useEffect(() => {
    // Hold acknowledged values until their listener snapshot arrives. Once clean,
    // later remote edits can refresh the editor without manufacturing a draft.
    if (pending.current) return;
    const latest = draftRef.current;
    if (
      latest &&
      (latest.epoch !== epoch ||
        (stableJSON(latest.value) === stableJSON(latest.base) &&
          version >= latest.version))
    )
      store(null);
  }, [epoch, version, stableJSON(saved), draft]);
  function change(next: SetStateAction<Adjustment>) {
    if (!enabled) return;
    const latest = draftRef.current?.epoch === epoch ? draftRef.current : null;
    const before = latest?.value ?? saved;
    store({
      epoch,
      value: structuredClone(typeof next === "function" ? next(before) : next),
      base: latest?.base ?? structuredClone(saved),
      version: latest?.version ?? version,
    });
    setError("");
    setMessage("");
  }
  function cancel() {
    if (pending.current) return;
    store(null);
    setError("");
    setMessage("");
  }
  function reset() {
    if (!pending.current) change(emptyAdjustment());
  }
  async function save() {
    const submitted = draftRef.current;
    if (
      pending.current ||
      busy ||
      !enabled ||
      !submitted ||
      submitted.epoch !== epoch ||
      stableJSON(submitted.value) === stableJSON(submitted.base) ||
      validateAdjustment(submitted.value, bounds.pre, bounds.post)
    )
      return;
    pending.current = true;
    setSaving(true);
    setError("");
    setMessage("");
    const snapshot = structuredClone(submitted.value);
    try {
      const acknowledged = await saveValue(key, snapshot, submitted.version);
      if (!alive.current || context.current.epoch !== epoch) return;
      const latest = draftRef.current!;
      store({ ...latest, base: snapshot, version: acknowledged });
      setMessage("Availability saved. Run Pebble to update results.");
    } catch (cause) {
      if (alive.current && context.current.epoch === epoch)
        setError(
          cause instanceof Error
            ? cause.message
            : "Could not save availability. Try again.",
        );
    } finally {
      pending.current = false;
      if (alive.current) setSaving(false);
    }
  }
  return {
    value,
    dirty,
    saving,
    blocked: saving || busy || !enabled,
    validation,
    error,
    message,
    change,
    cancel,
    reset,
    save,
  };
}
export type AvailabilityEditorState = ReturnType<typeof useAvailabilityEditor>;
