import type { ReactNode } from "react";
import {
  Sparkles,
  FileText,
  SlidersHorizontal,
  BarChart3,
  Users,
  ChevronRight,
  Check,
  Wifi,
  WifiOff,
  ShieldCheck,
} from "lucide-react";
import type { LiveConnection } from "../data/useFirestore";
import type { Page } from "../shared/model";
import { steps } from "./runStages";

type SidebarProps = {
  connection?: Pick<
    LiveConnection,
    "label" | "user" | "officer" | "signIn" | "signOut"
  >;
  canManage: boolean;
  page: Page;
  go: (page: Page) => void;
  reportCount: number;
  runButton: ReactNode;
  active: LiveConnection["active"];
  online: boolean;
  authPending: boolean;
  accountAction: (action: () => Promise<void>) => Promise<void>;
  guard: (action: () => void) => void;
  demoControls?: ReactNode;
};
export function Sidebar({
  connection,
  canManage,
  page,
  go,
  reportCount,
  runButton,
  active,
  online,
  authPending,
  accountAction,
  guard,
  demoControls,
}: SidebarProps) {
  return (
    <aside className="sidebar">
      <a
        className="brand"
        href="#reports"
        onClick={(e) => {
          e.preventDefault();
          go("reports");
        }}
      >
        <img src="/pebble.svg" width="36" height="36" alt="" />
        <span>pebble</span>
      </a>
      <p className="brand-subtitle">
        More time raiding,
        <br />
        less time in spreadsheets.
      </p>
      <div className="season-card">
        <span className="season-icon">
          <Sparkles size={18} />
        </span>
        <div>
          <strong>{connection?.label ?? "Midnight"}</strong>
          {!connection && <span>Season 1 · Sample data</span>}
        </div>
      </div>
      <nav aria-label="Main navigation">
        {(
          [
            { id: "reports", name: "Reports", icon: FileText },
            { id: "overrides", name: "Overrides", icon: SlidersHorizontal },
            { id: "results", name: "Results", icon: BarChart3 },
            ...(connection?.officer
              ? [
                  { id: "roster" as const, name: "Roster", icon: Users },
                  {
                    id: "settings" as const,
                    name: "Settings",
                    icon: SlidersHorizontal,
                  },
                ]
              : []),
          ] as const
        )
          .filter((item) => canManage || item.id === "results")
          .map((item) => (
            <button
              key={item.id}
              onClick={() => go(item.id)}
              className={`nav-item ${page === item.id ? "active" : ""}`}
              aria-current={page === item.id ? "page" : undefined}
            >
              <item.icon size={18} />
              <span>{item.name}</span>
              {item.id === "reports" ? (
                <span className="nav-count">{reportCount}</span>
              ) : page === item.id ? (
                <ChevronRight size={15} />
              ) : null}
            </button>
          ))}
      </nav>
      {(canManage || active) && (
        <div className="sidebar-run">
          {runButton}
          {active && (
            <div className="run-steps" aria-label="Processing stages">
              {steps.map((step, i) => (
                <span
                  key={step}
                  className={
                    i < active.stage
                      ? "done"
                      : i === active.stage
                        ? "current"
                        : ""
                  }
                >
                  {i < active.stage ? (
                    <Check size={12} />
                  ) : (
                    <span className="step-dot" />
                  )}
                  {step}
                </span>
              ))}
            </div>
          )}
        </div>
      )}
      <div className="sidebar-footer">
        {canManage && (
          <span className={`connection ${online ? "" : "offline"}`}>
            {online ? <Wifi size={14} /> : <WifiOff size={14} />}
            {online ? "Local bot online" : "Local bot offline"}
          </span>
        )}
        {demoControls}
        <div className="officer">
          <span className="avatar">
            {connection ? (
              connection.user ? (
                (connection.user.displayName || connection.user.email || "P")
                  .slice(0, 2)
                  .toUpperCase()
              ) : (
                <Users size={16} />
              )
            ) : (
              "MS"
            )}
          </span>
          <div>
            <strong>
              {connection
                ? connection.user
                  ? connection.user.displayName ||
                    connection.user.email ||
                    "Signed in"
                  : "Signed out"
                : "Example Officer"}
            </strong>
            <span>
              {connection
                ? connection.officer
                  ? "Officer"
                  : connection.user
                    ? "Officer access is not enabled"
                    : "Public results"
                : "Officer · Demo session"}
            </span>
            {connection && (
              <div className="account-actions">
                {!connection.user && (
                  <button
                    className="text-button"
                    disabled={authPending}
                    onClick={() => void accountAction(connection.signIn)}
                  >
                    Officer sign-in
                  </button>
                )}
                {connection.user && (
                  <button
                    className="text-button"
                    disabled={authPending}
                    onClick={() =>
                      guard(() => {
                        void accountAction(connection.signOut);
                      })
                    }
                  >
                    Sign out
                  </button>
                )}
              </div>
            )}
          </div>
          {canManage && <ShieldCheck size={17} />}
        </div>
      </div>
    </aside>
  );
}
