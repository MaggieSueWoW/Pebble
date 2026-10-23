import type { ReactNode } from "react";
import type { Page } from "../shared/model";

const titles: Record<Page, string> = {
  reports: "Reports",
  overrides: "Overrides",
  results: "Results",
  roster: "Roster",
  settings: "Settings",
};
/** Layout only; the workspace retains navigation and draft ownership. */
export function AppShell({
  page,
  sidebar,
  notices,
  children,
  overlays,
}: {
  page: Page;
  sidebar: ReactNode;
  notices?: ReactNode;
  children: ReactNode;
  overlays: ReactNode;
}) {
  return (
    <div className="app-shell">
      {sidebar}
      <main>
        <div
          className={`content ${page === "overrides" ? "overrides-page" : page === "roster" ? "roster-page" : page === "settings" ? "settings-page" : ""}`}
        >
          {notices}
          <div className="page-heading">
            <div>
              <h1 id="page-title" tabIndex={-1}>
                {titles[page]}
              </h1>
            </div>
          </div>
          {children}
        </div>
      </main>
      {overlays}
    </div>
  );
}
