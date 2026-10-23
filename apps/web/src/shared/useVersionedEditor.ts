import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type SetStateAction,
} from "react";
import { stableJSON } from "./model";

type Draft<T> = {
  epoch: number;
  value: T;
  base: T;
  version: number;
};

/** Share version-bound draft and acknowledgement behavior between input editors. */
export function useVersionedEditor<T>({
  key,
  saved,
  version,
  validate,
  saveValue,
  resetValue,
  busy = false,
  enabled = true,
  label,
}: {
  key: string;
  saved: T;
  version: number;
  validate: (value: T) => string | null;
  saveValue: (key: string, value: T, expected: number) => Promise<number>;
  resetValue: () => T;
  busy?: boolean;
  enabled?: boolean;
  label: string;
}) {
  const context = useRef({ key, epoch: 0 });
  if (context.current.key !== key)
    context.current = { key, epoch: context.current.epoch + 1 };
  const epoch = context.current.epoch;
  const [draft, setDraft] = useState<Draft<T> | null>(null);
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
  const validation = validate(value);
  const store = useCallback((next: Draft<T> | null) => {
    draftRef.current = next;
    setDraft(next);
  }, []);
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
  const savedSignature = stableJSON(saved);
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
  }, [epoch, version, savedSignature, draft, store]);
  function change(next: SetStateAction<T>) {
    if (!enabled) return;
    const latest = draftRef.current?.epoch === epoch ? draftRef.current : null;
    const before = latest?.value ?? saved;
    store({
      epoch,
      value: structuredClone(
        typeof next === "function" ? (next as (old: T) => T)(before) : next,
      ),
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
    if (!pending.current) change(resetValue());
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
      validate(submitted.value)
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
      setMessage(`${label} saved. Run Pebble to update results.`);
    } catch (cause) {
      if (alive.current && context.current.epoch === epoch)
        setError(
          cause instanceof Error
            ? cause.message
            : `Could not save ${label.toLowerCase()}. Try again.`,
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
