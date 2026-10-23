import { useCallback, useEffect, useRef, useState } from "react";
import type { Page } from "../shared/model";

export type ResultLocation = {
  page: Page;
  period: string;
  player: string | null;
};
const pages: Page[] = [
  "reports",
  "review",
  "overrides",
  "results",
  "roster",
  "settings",
];
const entry = () =>
  history.state?.pebbleNavigation as { index: number; page?: Page } | undefined;

export function readResultLocation(defaultPage: Page): ResultLocation {
  const params = new URLSearchParams(location.search);
  const savedPage = entry()?.page;
  const page =
    params.get("view") === "results"
      ? "results"
      : defaultPage !== "results" && savedPage && pages.includes(savedPage)
        ? savedPage
        : defaultPage;
  return {
    page,
    period: params.get("period") || "season",
    player: params.get("player") || null,
  };
}

/** Public result URLs identify a view within the deployment's configured season. */
export function resultLink(
  selection: Pick<ResultLocation, "period" | "player">,
  sample = false,
): string {
  const url = new URL(location.pathname, location.origin);
  if (sample) url.searchParams.set("demo", "1");
  url.searchParams.set("view", "results");
  url.searchParams.set("period", selection.period);
  if (selection.player) url.searchParams.set("player", selection.player);
  return url.href;
}

function navigationURL(selection: ResultLocation): string {
  if (selection.page === "results")
    return resultLink(
      selection,
      new URLSearchParams(location.search).get("demo") === "1",
    );
  const url = new URL(location.href);
  for (const key of ["view", "period", "player"]) url.searchParams.delete(key);
  url.hash = "";
  return url.href;
}

/** Keep result selection in browser history; restore a guarded traversal before asking to discard. */
export function useResultNavigation(
  defaultPage: Page,
  blocked: () => boolean,
  guard: (action: () => void) => void,
) {
  const [selection, setSelection] = useState(() =>
    readResultLocation(defaultPage),
  );
  const current = useRef({
    selection,
    index: entry()?.index ?? 0,
  });
  const callbacks = useRef({ blocked, guard, defaultPage });
  callbacks.current = { blocked, guard, defaultPage };
  const restored = useRef<(() => void) | null>(null);
  const approved = useRef(false);

  const navigate = useCallback(
    (next: Partial<ResultLocation>, replace = false) => {
      const value = { ...current.current.selection, ...next };
      const url = navigationURL(value);
      const index = current.current.index + (replace ? 0 : 1);
      const state = {
        ...history.state,
        pebbleNavigation: { index, page: value.page },
      };
      history[replace ? "replaceState" : "pushState"](state, "", url);
      current.current = { selection: value, index };
      setSelection(value);
    },
    [],
  );

  useEffect(() => {
    if (!entry())
      history.replaceState(
        {
          ...history.state,
          pebbleNavigation: { index: current.current.index },
        },
        "",
      );
    const pop = () => {
      if (restored.current) {
        const request = restored.current;
        restored.current = null;
        request();
        return;
      }
      const index = entry()?.index ?? 0;
      const next = readResultLocation(callbacks.current.defaultPage);
      const delta = index - current.current.index;
      if (!approved.current && callbacks.current.blocked() && delta) {
        // Put the address/history cursor back before opening the existing draft guard.
        restored.current = () =>
          callbacks.current.guard(() => {
            approved.current = true;
            history.go(delta);
          });
        history.go(-delta);
        return;
      }
      approved.current = false;
      current.current = { selection: next, index };
      setSelection(next);
    };
    window.addEventListener("popstate", pop);
    return () => window.removeEventListener("popstate", pop);
  }, []);

  return { ...selection, navigate };
}
