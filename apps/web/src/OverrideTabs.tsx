import { useEffect, useRef, useState } from "react";
import { Clock3, Users } from "lucide-react";

type Tab = "availability" | "timing";
const tabs = [
  { id: "availability", label: "Player availability", icon: Users },
  { id: "timing", label: "Raid timing", icon: Clock3 },
] as const;

/** Move focus independently of guarded editor activation. */
export function OverrideTabs({
  value,
  onChange,
}: {
  value: Tab;
  onChange: (tab: Tab) => void;
}) {
  const [focused, setFocused] = useState<Tab>(value);
  const buttons = useRef<(HTMLButtonElement | null)[]>([]);
  useEffect(() => setFocused(value), [value]);
  return (
    <div className="tabs" role="tablist" aria-label="Override type">
      {tabs.map((tab, index) => (
        <button
          key={tab.id}
          ref={(node) => {
            buttons.current[index] = node;
          }}
          id={`override-tab-${tab.id}`}
          role="tab"
          aria-selected={value === tab.id}
          aria-controls="override-panel"
          tabIndex={focused === tab.id ? 0 : -1}
          className={value === tab.id ? "selected" : ""}
          onFocus={() => setFocused(tab.id)}
          onClick={() => {
            if (value !== tab.id) onChange(tab.id);
          }}
          onKeyDown={(event) => {
            let next: number;
            switch (event.key) {
              case "ArrowLeft":
                next = (index + tabs.length - 1) % tabs.length;
                break;
              case "ArrowRight":
                next = (index + 1) % tabs.length;
                break;
              case "Home":
                next = 0;
                break;
              case "End":
                next = tabs.length - 1;
                break;
              default:
                return;
            }
            event.preventDefault();
            buttons.current[next]?.focus();
          }}
        >
          <tab.icon size={16} aria-hidden="true" /> {tab.label}
        </button>
      ))}
    </div>
  );
}
