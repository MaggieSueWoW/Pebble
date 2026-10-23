import { ExternalLink } from "lucide-react";
import type { LiveConnection } from "../../data/useFirestore";

import { terseWarning, warningsForNight } from "./warnings";
export function ResultWarnings({
  warnings,
  sources = [],
  compact = false,
}: {
  compact?: boolean;
  warnings: ReturnType<typeof warningsForNight>;
  sources?: LiveConnection["resultSources"];
}) {
  if (!warnings.length) return null;
  return (
    <section className="notice amber" aria-label="Result warnings">
      <div>
        <h3>Result warnings</h3>
        {!compact && <p>From the displayed completed run.</p>}
        <ul>
          {warnings.map(({ text, index }) => (
            <li key={index}>
              {compact ? terseWarning(text) : text}
              {sources.some((source) =>
                source.warningIndexes?.includes(index),
              ) && (
                <ul aria-label="Source reports for this warning">
                  {sources
                    .filter((source) => source.warningIndexes?.includes(index))
                    .map((source) => (
                      <li key={source.code}>
                        <a
                          href={`https://www.warcraftlogs.com/reports/${source.code}`}
                          target="_blank"
                          rel="noreferrer"
                        >
                          {source.title}{" "}
                          <ExternalLink size={12} aria-hidden="true" />
                        </a>
                      </li>
                    ))}
                </ul>
              )}
            </li>
          ))}
        </ul>
      </div>
    </section>
  );
}
