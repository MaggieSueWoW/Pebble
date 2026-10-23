import { useEffect, useRef, useState } from "react";
import { parseReports, type Report } from "../../shared/model";
import type { Versions } from "../../data/useFirestore";

/** Keep report drafts and their version baselines until a save is acknowledged. */
export function useReportEditor(
  reports: Report[],
  versions: Versions,
  save: (changes: Report[], expected: Versions) => Promise<void>,
  busy = false,
  demoNight = "",
) {
  const [text, updateText] = useState("");
  const [edits, setEdits] = useState<Record<string, boolean>>({});
  const expected = useRef<Versions>({});
  const revision = useRef(0);
  const pending = useRef(false);
  const alive = useRef(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  useEffect(() => {
    alive.current = true;
    return () => {
      alive.current = false;
    };
  }, []);
  const parsed = parseReports(text, reports);
  const ready = parsed.filter((p) => p.state === "ready");
  function setText(value: string) {
    revision.current += 1;
    updateText(value);
    setMessage("");
  }
  function cancelSelection() {
    if (pending.current) return;
    setEdits({});
    expected.current = {};
    setError("");
    setMessage("");
  }
  async function commit(
    changes: Report[],
    baseline: Versions,
    done: () => void,
    success: string,
  ) {
    if (pending.current || busy || !changes.length) return;
    pending.current = true;
    setSaving(true);
    setError("");
    setMessage("");
    try {
      await save(changes, baseline);
      if (alive.current) {
        done();
        setMessage(success);
      }
    } catch (cause) {
      if (alive.current)
        setError(
          cause instanceof Error
            ? cause.message
            : "Could not save reports. Try again.",
        );
    } finally {
      pending.current = false;
      if (alive.current) setSaving(false);
    }
  }
  function add() {
    if (parsed.some((p) => p.state === "invalid")) return Promise.resolve();
    const submitted = revision.current;
    return commit(
      ready.map((p) => ({
        code: p.code,
        enabled: true,
        fetched: false,
        title: "",
        night: demoNight,
      })),
      Object.fromEntries(ready.map((p) => [`reports/${p.code}`, 0])),
      () => {
        if (revision.current === submitted) setText("");
      },
      "Reports added. Run Pebble when you’re ready.",
    );
  }
  function reenable(code: string) {
    const report = reports.find((r) => r.code === code);
    if (!report || report.enabled) return Promise.resolve();
    const submitted = revision.current;
    return commit(
      [{ ...report, enabled: true }],
      { [`reports/${code}`]: versions[`reports/${code}`] ?? 0 },
      () => {
        if (revision.current === submitted)
          setText(
            text
              .split(/\r?\n/)
              .filter((line) => parseReports(line, reports)[0]?.code !== code)
              .join("\n"),
          );
        setEdits((old) => {
          const next = { ...old };
          delete next[code];
          return next;
        });
        delete expected.current[`reports/${code}`];
      },
      "Report included again. Run Pebble to update results.",
    );
  }
  function toggle(code: string) {
    if (pending.current || busy) return;
    const report = reports.find((r) => r.code === code);
    if (!report) return;
    const enabled = !(edits[code] ?? report.enabled);
    const next = { ...edits };
    if (code in next) {
      // Toggling a draft twice returns to the latest saved selection.
      delete next[code];
      delete expected.current[`reports/${code}`];
    } else {
      next[code] = enabled;
      expected.current[`reports/${code}`] = versions[`reports/${code}`] ?? 0;
    }
    setEdits(next);
    setError("");
    setMessage("");
  }
  function saveSelection() {
    return commit(
      reports
        .filter((r) => r.code in edits)
        .map((r) => ({ ...r, enabled: edits[r.code] })),
      { ...expected.current },
      () => {
        setEdits({});
        expected.current = {};
      },
      "Report selection saved. Run Pebble to update results.",
    );
  }
  function reset() {
    if (pending.current) return;
    setText("");
    cancelSelection();
  }
  return {
    text,
    setText,
    edits,
    parsed,
    ready,
    saving,
    blocked: saving || busy,
    error,
    message,
    dirty: Boolean(text.trim()) || Object.keys(edits).length > 0,
    add,
    reenable,
    toggle,
    cancelSelection,
    saveSelection,
    reset,
  };
}
export type ReportEditor = ReturnType<typeof useReportEditor>;
