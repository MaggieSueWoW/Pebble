import type { Page } from "../shared/model";
const flow = [
  ["reports", "Add report"],
  ["review", "Review Night"],
  ["overrides", "Overrides"],
  ["results", "Results"],
] as const;
export function NightFlow({
  page,
  go,
}: {
  page: Page;
  go: (page: Page) => void;
}) {
  return (
    <nav className="night-flow" aria-label="Process a raid night">
      <ol>
        {flow.map(([target, label], index) => (
          <li key={target}>
            <button
              aria-current={page === target ? "step" : undefined}
              onClick={() => go(target)}
            >
              <span className="flow-number">{index + 1}</span>
              {label}
            </button>
          </li>
        ))}
      </ol>
    </nav>
  );
}
