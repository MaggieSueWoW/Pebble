import { FlaskConical, ChevronDown, RotateCcw } from "lucide-react";
import type { DemoSession } from "./useDemoSession";

export function DemoControls({
  session,
  onReset,
}: {
  session: DemoSession;
  onReset: () => void;
}) {
  const { online, setOnline, active, failNext, setFailNext } = session;
  return (
    <details className="demo-controls">
      <summary>
        <FlaskConical size={15} /> Prototype controls <ChevronDown size={13} />
      </summary>
      <p>Everything here is sample data, saved in this browser.</p>
      <label className="control-line">
        <span>Bot online</span>
        <input
          type="checkbox"
          checked={online}
          disabled={active?.status === "running"}
          onChange={(e) => setOnline(e.target.checked)}
        />
      </label>
      <label className="control-line">
        <span>Fail next run</span>
        <input
          type="checkbox"
          checked={failNext}
          onChange={(e) => setFailNext(e.target.checked)}
        />
      </label>
      <button
        disabled={Boolean(active)}
        className="text-button"
        onClick={onReset}
      >
        <RotateCcw size={13} /> Reset sample data
      </button>
    </details>
  );
}
