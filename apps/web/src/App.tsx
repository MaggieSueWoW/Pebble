import { authMessage } from "./authMessages";
import type { LiveConnection } from "./useFirestore";
import { useEffect, useRef, useState, type ReactNode } from "react";
import {
  Activity,
  ArrowDown,
  ArrowRight,
  ArrowUpRight,
  BarChart3,
  Check,
  CheckCheck,
  ChevronDown,
  ChevronRight,
  CircleAlert,
  Clock3,
  FileText,
  ExternalLink,
  FlaskConical,
  Info,
  LoaderCircle,
  Play,
  RotateCcw,
  Search,
  Settings2,
  ShieldCheck,
  SlidersHorizontal,
  Sparkles,
  Users,
  Wifi,
  WifiOff,
  X,
} from "lucide-react";
import {
  adjustmentKey,
  calculate,
  defaultTiming,
  emptyAdjustment,
  formatDate,
  formatTime,
  freshState,
  minutes,
  playerDetails,
  nightOptions as sampleNights,
  players as samplePlayers,
  signature,
  timingSpans,
  type Inputs,
  type Page,
  type Saved,
} from "./model";

import { useReportEditor } from "./useReportEditor";
import { ReportsPanel } from "./ReportsPanel";
import { AvailabilityEditor } from "./AvailabilityEditor";
import { useAvailabilityEditor } from "./useAvailabilityEditor";
import { TimingEditor } from "./TimingEditor";
import { OverrideTabs } from "./OverrideTabs";
import { useTimingEditor } from "./useTimingEditor";
import { blankTiming } from "./timing";

const STORAGE = "pebble-prototype-v1";
const formatRunDate = (iso: string) =>
  new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    timeZone: "America/Los_Angeles",
  }).format(new Date(iso));
function loadSaved(): Saved {
  try {
    const stored = JSON.parse(localStorage.getItem(STORAGE) || "null");
    if (
      stored?.schema === 1 &&
      Array.isArray(stored.data?.inputs?.reports) &&
      Array.isArray(stored.data?.results?.nights) &&
      Array.isArray(stored.data?.history)
    )
      return stored.data;
  } catch {
    /* Storage can be blocked or stale; the demo can still run in memory. */
  }
  return freshState();
}
const steps = [
  "Queued",
  "Validating inputs",
  "Fetching reports",
  "Calculating results",
  "Updating results",
];
type ActiveRun = {
  id: string;
  status: "queued" | "running";
  stage: number;
  snapshot?: Inputs;
  fail?: boolean;
};
function Badge({
  children,
  tone = "neutral",
}: {
  children: ReactNode;
  tone?: "neutral" | "green" | "amber" | "purple" | "red";
}) {
  return <span className={`badge ${tone}`}>{children}</span>;
}
function Notice({
  children,
  tone = "neutral",
}: {
  children: ReactNode;
  tone?: string;
}) {
  return (
    <div className={`notice ${tone}`}>
      <Info size={16} />
      <span>{children}</span>
    </div>
  );
}
function Stat({
  label,
  value,
  detail,
  icon,
}: {
  label: string;
  value: ReactNode;
  detail: string;
  icon: ReactNode;
}) {
  return (
    <div className="stat">
      <span className="stat-icon">{icon}</span>
      <div>
        <span className="eyebrow">{label}</span>
        <div className="stat-number">{value}</div>
        <span className="muted small">{detail}</span>
      </div>
    </div>
  );
}
function ConfirmDiscard({
  action,
  cancel,
  discard,
}: {
  action: (() => void) | null;
  cancel: () => void;
  discard: () => void;
}) {
  const ref = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    if (action) ref.current?.showModal();
    else ref.current?.close();
  }, [action]);
  return (
    <dialog
      ref={ref}
      className="confirm-dialog"
      onCancel={(e) => {
        e.preventDefault();
        cancel();
      }}
    >
      <h2>Keep your changes?</h2>
      <p>
        You have unsaved edits. Save them before leaving, or discard them to
        continue.
      </p>
      <div className="button-row">
        <button className="button secondary" onClick={cancel}>
          Keep editing
        </button>
        <button className="button primary" onClick={discard}>
          Discard edits
        </button>
      </div>
    </dialog>
  );
}
export default function App({ connection }: { connection?: LiveConnection }) {
  const [demoSaved, setSaved] = useState<Saved>(
    () => connection?.saved ?? loadSaved(),
  );
  const saved = connection?.saved ?? demoSaved;
  const { inputs, results, history } = saved;
  const canManage = !connection || connection.officer;
  const [authPending, setAuthPending] = useState(false);
  const authActionPending = useRef(false);
  async function accountAction(action: () => Promise<void>) {
    if (authActionPending.current) return;
    authActionPending.current = true;
    setAuthPending(true);
    try {
      await action();
    } catch (error) {
      notify(authMessage(error), true);
    } finally {
      authActionPending.current = false;
      setAuthPending(false);
    }
  }
  const players = connection?.players ?? samplePlayers;
  const todayParts = Object.fromEntries(
    new Intl.DateTimeFormat("en-US", {
      timeZone: "America/Los_Angeles",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    })
      .formatToParts(new Date())
      .map(({ type, value }) => [type, value]),
  );
  const today = `${todayParts.year}-${todayParts.month}-${todayParts.day}`;
  const nightOptions = connection
    ? [
        ...new Set(
          [
            ...connection.nights,
            ...inputs.reports.map((report) => report.night),
          ].filter(Boolean),
        ),
      ]
        .sort()
        .reverse()
    : sampleNights;
  const defaultNight = nightOptions[0] ?? today;
  const firstPlayer = players[0]?.id ?? "";
  const [page, setPage] = useState<Page>(canManage ? "reports" : "results");
  const [night, setNight] = useState(defaultNight);
  const nightChosen = useRef(false);
  const [overrideTab, setOverrideTab] = useState<"availability" | "timing">(
    "availability",
  );
  const [selected, setSelected] = useState(firstPlayer);
  const [search, setSearch] = useState("");
  const [resultNight, setResultNight] = useState("season");
  const [expandedPlayer, setExpandedPlayer] = useState<string | null>(null);
  const [demoOnline, setOnline] = useState(true);
  const online = connection?.online ?? demoOnline;
  const [failNext, setFailNext] = useState(false);
  const [demoActive, setActive] = useState<ActiveRun | null>(null);
  const active: ActiveRun | null = connection ? connection.active : demoActive;
  const [toast, setToast] = useState<{ text: string; error?: boolean } | null>(
    null,
  );
  const [pendingAction, setPendingAction] = useState<(() => void) | null>(null);
  const [storageError, setStorageError] = useState(false);
  const inputRef = useRef(inputs);
  const failRef = useRef(failNext);
  inputRef.current = inputs;
  failRef.current = failNext;
  const toastTimer = useRef<ReturnType<typeof setTimeout> | undefined>(
    undefined,
  );
  const reportEditor = useReportEditor(
    inputs.reports,
    connection?.versions ?? {},
    async (reports, expected) => {
      if (connection)
        await connection.saveInputs(
          { reports, adjustments: {}, timings: {} },
          expected,
        );
      else
        setSaved((old) => ({
          ...old,
          inputs: {
            ...old.inputs,
            reports: [
              ...reports.filter(
                (r) =>
                  !old.inputs.reports.some(
                    (existing) => existing.code === r.code,
                  ),
              ),
              ...old.inputs.reports.map(
                (r) => reports.find((change) => change.code === r.code) ?? r,
              ),
            ],
          },
        }));
    },
    Boolean(connection?.busy),
    connection ? "" : defaultNight,
  );
  let span: { pre: number; post: number } = { pre: 90, post: 80 };
  try {
    const liveNight = results.nights.find((n) => n.night === night) as
      | ((typeof results.nights)[number] & {
          preMinutes?: number;
          postMinutes?: number;
        })
      | undefined;
    span = connection
      ? {
          pre: liveNight?.preMinutes ?? 720,
          post: liveNight?.postMinutes ?? 720,
        }
      : timingSpans(night, inputs.timings[night]);
  } catch {
    /* The saved timing error is surfaced by the run. */
  }
  const availabilityKey = adjustmentKey(night, selected);
  const availabilityPath = `availabilityOverrides/${night}_${selected}`;
  const availabilityEditor = useAvailabilityEditor(
    availabilityKey,
    inputs.adjustments[availabilityKey] ?? emptyAdjustment(),
    connection?.versions[availabilityPath] ?? 0,
    span,
    async (key, adjustment, expected) => {
      const slash = key.indexOf("/");
      const path = `availabilityOverrides/${key.slice(0, slash)}_${key.slice(slash + 1)}`;
      if (connection) {
        const acknowledged = await connection.saveInputs(
          { reports: [], adjustments: { [key]: adjustment }, timings: {} },
          { [path]: expected },
        );
        return acknowledged[path] ?? 0;
      }
      setSaved((old) => ({
        ...old,
        inputs: {
          ...old.inputs,
          adjustments: { ...old.inputs.adjustments, [key]: adjustment },
        },
      }));
      return expected + 1;
    },
    Boolean(connection?.busy),
    players.some((player) => player.id === selected),
  );
  const selectedPlayer = players.find((player) => player.id === selected);
  useEffect(() => {
    if (
      !selectedPlayer &&
      !availabilityEditor.dirty &&
      !availabilityEditor.saving
    )
      setSelected(firstPlayer);
  }, [
    selectedPlayer,
    firstPlayer,
    availabilityEditor.dirty,
    availabilityEditor.saving,
  ]);
  const detectedTiming = connection
    ? (results.nights.find((item) => item.night === night)?.timing ??
      blankTiming())
    : defaultTiming(night);
  const timingEditor = useTimingEditor(
    night,
    inputs.timings[night] ?? blankTiming(),
    connection?.versions[`nightOverrides/${night}`] ?? 0,
    detectedTiming,
    async (key, timing, expected) => {
      if (connection) {
        const versions = await connection.saveInputs(
          { reports: [], adjustments: {}, timings: { [key]: timing } },
          { [`nightOverrides/${key}`]: expected },
        );
        return versions[`nightOverrides/${key}`] ?? 0;
      }
      setSaved((old) => ({
        ...old,
        inputs: {
          ...old.inputs,
          timings: { ...old.inputs.timings, [key]: timing },
        },
      }));
      return expected + 1;
    },
    Boolean(connection?.busy),
    !connection,
  );
  const unsaved =
    reportEditor.dirty ||
    reportEditor.saving ||
    availabilityEditor.dirty ||
    availabilityEditor.saving ||
    timingEditor.dirty ||
    timingEditor.saving;
  useEffect(() => {
    // Follow arriving metadata only until the operator chooses or edits a date.
    if (unsaved) nightChosen.current = true;
    if (!nightChosen.current) setNight(defaultNight);
  }, [defaultNight, unsaved]);
  const unprocessed = connection
    ? connection.unprocessed
    : results.signature !== signature(inputs);
  const included = inputs.reports.filter((r) => r.enabled);
  const visiblePlayers = players.filter((p) =>
    `${p.name} ${p.className}`.toLowerCase().includes(search.toLowerCase()),
  );

  function notify(text: string, error = false) {
    clearTimeout(toastTimer.current);
    setToast({ text, error });
    toastTimer.current = setTimeout(() => setToast(null), 6000);
  }
  useEffect(() => () => clearTimeout(toastTimer.current), []);
  useEffect(() => {
    window.scrollTo({ top: 0, behavior: "instant" });
  }, [page]);
  useEffect(() => {
    if (connection) return;
    try {
      localStorage.setItem(STORAGE, JSON.stringify({ schema: 1, data: saved }));
      setStorageError(false);
    } catch {
      setStorageError(true);
    }
  }, [saved]);
  useEffect(() => {
    const handle = (e: BeforeUnloadEvent) => {
      if (unsaved) {
        e.preventDefault();
      }
    };
    window.addEventListener("beforeunload", handle);
    return () => window.removeEventListener("beforeunload", handle);
  }, [unsaved]);
  useEffect(() => {
    if (connection || !active || active.status !== "queued" || !online) return;
    const timer = setTimeout(() => {
      setActive((current) =>
        current
          ? {
              ...current,
              status: "running",
              stage: 1,
              snapshot: structuredClone(inputRef.current),
              fail: failRef.current,
            }
          : null,
      );
      setFailNext(false);
    }, 650);
    return () => clearTimeout(timer);
  }, [active?.id, active?.status, online]);
  useEffect(() => {
    if (
      connection ||
      !active ||
      active.status !== "running" ||
      !active.snapshot
    )
      return;
    const current = active;
    const timers = [1400, 2600, 3800].map((time, i) =>
      setTimeout(
        () => setActive((r) => (r ? { ...r, stage: i + 2 } : null)),
        time,
      ),
    );
    timers.push(
      setTimeout(() => {
        const at = new Date().toISOString();
        try {
          if (current.fail)
            throw new Error(
              "The demo bot could not reach Warcraft Logs. Previous results are still available. Try running again.",
            );
          const nextResults = calculate(current.snapshot!, at);
          const nextInputs = {
            ...inputRef.current,
            reports: inputRef.current.reports.map((r) =>
              current.snapshot!.reports.some(
                (s) => s.code === r.code && s.enabled,
              )
                ? { ...r, fetched: true }
                : r,
            ),
          };
          // Fetched is UI metadata, so carry that update into the consumed snapshot too.
          const consumed = {
            ...current.snapshot!,
            reports: current.snapshot!.reports.map((r) =>
              r.enabled ? { ...r, fetched: true } : r,
            ),
          };
          nextResults.signature = signature(consumed);
          setSaved((old) => ({
            inputs: nextInputs,
            results: nextResults,
            history: [
              {
                id: current.id,
                at,
                status: "succeeded" as const,
                message: `${nextResults.reportCount} reports processed · ${nextResults.nights.length} raid nights`,
              },
              ...old.history,
            ].slice(0, 12),
          }));
          notify("Results updated. Your team would see this result set now.");
        } catch (e) {
          const message = (e as Error).message;
          setSaved((old) => ({
            ...old,
            history: [
              { id: current.id, at, status: "failed" as const, message },
              ...old.history,
            ].slice(0, 12),
          }));
          notify(message, true);
        }
        setActive(null);
      }, 4800),
    );
    return () => timers.forEach(clearTimeout);
  }, [active?.id, active?.status]);

  function clearEdits() {
    reportEditor.reset();
    availabilityEditor.cancel();
    timingEditor.cancel();
  }
  function guard(action: () => void) {
    if (
      reportEditor.saving ||
      availabilityEditor.saving ||
      timingEditor.saving
    ) {
      notify("Wait for the save to finish.");
      return;
    }
    if (unsaved) setPendingAction(() => action);
    else action();
  }
  function go(next: Page) {
    if (!canManage && next !== "results") return;
    if (next !== page) guard(() => setPage(next));
  }
  function chooseNight(next: string) {
    if (next === night) {
      nightChosen.current = true;
      return;
    }
    guard(() => {
      nightChosen.current = true;
      setNight(next);
    });
  }
  function choosePlayer(id: string) {
    if (id === selected) return;
    guard(() => {
      setSelected(id);
    });
  }
  async function startRun() {
    if (active || unsaved || !canManage) return;
    if (connection) {
      try {
        await connection.requestRun();
      } catch (error) {
        notify((error as Error).message, true);
      }
      return;
    }
    setActive({ id: crypto.randomUUID(), status: "queued", stage: 0 });
  }
  const runButton = canManage ? (
    <button
      className="button primary run-button"
      onClick={startRun}
      disabled={
        Boolean(active) ||
        unsaved ||
        Boolean(connection?.busy) ||
        Boolean(connection?.submitting) ||
        Boolean(connection && !connection.queueReady) ||
        !canManage
      }
      title={
        unsaved
          ? "Save or discard your edits first"
          : active
            ? "A run is already pending"
            : "Compute and update results"
      }
    >
      {active || connection?.submitting ? (
        <LoaderCircle size={16} className="spin" />
      ) : (
        <Play size={15} fill="currentColor" />
      )}
      {active
        ? active.status === "queued"
          ? "Run queued"
          : "Running Pebble"
        : connection?.submitting
          ? "Submitting run…"
          : "Run Pebble"}
    </button>
  ) : (
    <button
      className="button primary"
      disabled={authPending}
      onClick={() => {
        const action = connection?.user
          ? connection.signOut
          : connection?.signIn;
        if (action) void accountAction(action);
      }}
    >
      {authPending
        ? "Please wait…"
        : connection?.user
          ? "Sign out to switch accounts"
          : "Officer sign-in"}
    </button>
  );
  const visibleResultNight = results.nights.some((n) => n.night === resultNight)
    ? resultNight
    : "season";
  useEffect(() => {
    if (
      resultNight !== "season" &&
      !results.nights.some((n) => n.night === resultNight)
    ) {
      setResultNight("season");
      setExpandedPlayer(null);
    }
  }, [results.nights, resultNight]);
  const latestNight = results.nights.find((n) => n.night === night);
  const resultRows = (
    visibleResultNight === "season"
      ? results.players
      : (results.nights.find((n) => n.night === visibleResultNight)?.players ??
        [])
  )
    .filter((r) => r.played + r.bench > 0)
    .sort(
      (a, b) =>
        a.bench - b.bench ||
        (players.find((p) => p.id === a.id)?.name ?? a.id).localeCompare(
          players.find((p) => p.id === b.id)?.name ?? b.id,
        ),
    );
  const lastFailed = history[0]?.status === "failed";
  const waitingForResults = Boolean(
    connection?.submitting || connection?.resultsPending,
  );
  const resultLoadFailed = Boolean(connection?.resultsError);
  return (
    <div className="app-shell">
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
          <span>
            pebble<span className="brand-dot">.</span>
          </span>
        </a>
        <div className="workspace-label">
          {canManage ? "OFFICER WORKSPACE" : "TEAM RESULTS"}
        </div>
        <div className="season-card">
          <span className="season-icon">
            <Sparkles size={18} />
          </span>
          <div>
            <strong>{connection?.label ?? "Midnight"}</strong>
            <span>
              {connection ? "Raid time fairness" : "Season 1 · Sample data"}
            </span>
          </div>
        </div>
        <div className="nav-group-label">MANAGE YOUR RAID</div>
        <nav aria-label="Main navigation">
          {(
            [
              { id: "reports", name: "Reports", icon: FileText },
              { id: "overrides", name: "Overrides", icon: SlidersHorizontal },
              { id: "results", name: "Results", icon: BarChart3 },
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
                  <span className="nav-count">{inputs.reports.length}</span>
                ) : page === item.id ? (
                  <ChevronRight size={15} />
                ) : null}
              </button>
            ))}
        </nav>
        <div className="sidebar-note">
          <span className="tiny-pebble" />
          <p>
            More time raiding.
            <br />
            <strong>Less time in spreadsheets.</strong>
          </p>
        </div>
        {!connection && (
          <details className="demo-controls">
            <summary>
              <FlaskConical size={15} /> Prototype controls{" "}
              <ChevronDown size={13} />
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
              onClick={() =>
                guard(() => {
                  setSaved(freshState());
                  nightChosen.current = false;
                  setNight(defaultNight);
                  setSelected("Example");
                  availabilityEditor.cancel();
                  timingEditor.cancel();
                  setResultNight("season");
                  setFailNext(false);
                  setOnline(true);
                  notify("Sample data reset.");
                })
              }
            >
              <RotateCcw size={13} /> Reset sample data
            </button>
          </details>
        )}
        <div className="officer">
          <span className="avatar">
            {connection
              ? connection.user?.displayName?.slice(0, 2).toUpperCase() || "P"
              : "MS"}
          </span>
          <div>
            <strong>
              {connection
                ? connection.user?.displayName || "Team member"
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
            {connection?.user && (
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
            {connection?.user && (
              <button
                className="text-button"
                disabled={authPending}
                onClick={() =>
                  guard(() => {
                    void accountAction(connection.signIn);
                  })
                }
              >
                Switch Google account
              </button>
            )}
          </div>
          <ShieldCheck size={17} />
        </div>
      </aside>
      <main>
        <header className="topbar">
          <div className="breadcrumb">
            Workspace <ChevronRight size={13} />
            <strong>
              {page === "reports"
                ? "Reports"
                : page === "overrides"
                  ? "Overrides"
                  : "Results"}
            </strong>
          </div>
          <div className="topbar-right">
            <span className="prototype-label">
              <FlaskConical size={13} />{" "}
              {connection ? connection.label : "INTERACTIVE PROTOTYPE"}
            </span>
            <span className={`connection ${online ? "" : "offline"}`}>
              {online ? <Wifi size={14} /> : <WifiOff size={14} />}
              {!canManage
                ? "Public results"
                : online
                  ? "Local bot online"
                  : "Local bot offline"}
            </span>
          </div>
        </header>
        <div className="content">
          {connection?.error && (
            <Notice tone="amber">{connection.error}</Notice>
          )}
          {connection?.accessError && (
            <Notice tone="amber">
              <span role="alert">{connection.accessError}</span>{" "}
              <button className="text-button" onClick={connection.retryAccess}>
                Retry officer access
              </button>
            </Notice>
          )}
          {connection?.user &&
            !connection.officer &&
            !connection.accessError && (
              <section
                className="notice"
                aria-labelledby="officer-access-title"
              >
                <Info size={16} aria-hidden="true" />
                <div>
                  <h2 id="officer-access-title">Officer access not enabled</h2>
                  <p>
                    This account does not have officer access. You can still
                    read completed results. Editing inputs and running Pebble
                    require an enabled officer account. Switch Google account or
                    sign out to use another account.
                  </p>
                  <details>
                    <summary>Account details</summary>
                    <p>
                      Account ID: <code>{connection.user.uid}</code>
                    </p>
                    <p>Use this ID when enabling officer access.</p>
                  </details>
                </div>
              </section>
            )}
          {connection?.resultsError && (
            <Notice tone="amber">
              <span role="alert">{connection.resultsError}</span>{" "}
              <button className="text-button" onClick={connection.retryResults}>
                Retry loading results
              </button>
            </Notice>
          )}
          {connection &&
            !connection.hasResults &&
            !connection.resultsPending &&
            !connection.resultsError && (
              <Notice>
                No completed results yet.{" "}
                {canManage
                  ? "Add or include reports, then run Pebble to create the first result set."
                  : "Completed results will appear here after an officer runs Pebble."}
              </Notice>
            )}
          <div className="page-heading">
            <div>
              <div className="eyebrow accent">
                {page === "results"
                  ? "THE LATEST PICTURE"
                  : "A LITTLE LESS RAID ADMIN"}
              </div>
              <h1>
                {page === "reports"
                  ? "Raid reports"
                  : page === "overrides"
                    ? "Fine-tune the night"
                    : "Everyone’s time counts."}
              </h1>
              <p>
                {page === "reports"
                  ? "Bring the logs. Pebble handles the numbers."
                  : page === "overrides"
                    ? "A few adjustments, then back to the raid."
                    : "Played time, bench time, and the story behind the numbers."}
              </p>
            </div>
            {page === "results" ? (
              canManage && (
                <button
                  className="button secondary"
                  onClick={() => go("reports")}
                >
                  <FileText size={16} /> Manage reports
                </button>
              )
            ) : (
              <button
                className="button secondary"
                onClick={() => go("results")}
              >
                View results <ArrowUpRight size={16} />
              </button>
            )}
          </div>
          <div
            className={`run-strip ${active || connection?.submitting || waitingForResults ? "is-running" : ""} ${(lastFailed || resultLoadFailed) && !active && !connection?.submitting ? "has-error" : ""}`}
            role="status"
            aria-label="Run status"
          >
            <div
              className={`strip-icon ${active || connection?.submitting || waitingForResults ? "running-icon" : ""}`}
            >
              {active ||
              connection?.submitting ||
              (waitingForResults && !resultLoadFailed) ? (
                <LoaderCircle size={19} className="spin" />
              ) : lastFailed || resultLoadFailed ? (
                <CircleAlert size={19} />
              ) : unprocessed ? (
                <Clock3 size={19} />
              ) : (
                <CheckCheck size={19} />
              )}
            </div>
            <div className="strip-copy">
              <strong>
                {active
                  ? active.status === "queued" && !online
                    ? "Queued · waiting for your local bot"
                    : steps[active.stage] + "…"
                  : connection?.submitting
                    ? "Submitting run…"
                    : connection?.officer && !connection.queueReady
                      ? connection.queueError
                        ? "Run queue is unavailable"
                        : "Checking run queue…"
                      : connection?.resultsError
                        ? "Latest results are unavailable"
                        : connection?.resultsPending
                          ? "Loading latest results…"
                          : lastFailed
                            ? "Last run didn’t finish"
                            : unsaved
                              ? "You have unsaved edits"
                              : unprocessed
                                ? "Changes ready for the next run"
                                : connection && !connection.hasResults
                                  ? "No completed runs yet"
                                  : connection &&
                                      history[0]?.status === "succeeded"
                                    ? "Results updated"
                                    : "You’re all caught up"}
              </strong>
              <span>
                {active
                  ? "Your last completed results stay visible while Pebble works."
                  : connection?.submitting
                    ? "Waiting for the server to confirm your request."
                    : connection?.officer && !connection.queueReady
                      ? "Waiting to check for pending runs. Your previous results stay visible."
                      : connection?.resultsError || connection?.resultsPending
                        ? connection.hasResults
                          ? "Showing the previous completed results until the latest set loads."
                          : "The latest result set has not loaded yet."
                        : lastFailed
                          ? "Previous results are still available. You can run Pebble again."
                          : unsaved
                            ? "Save or discard your edits before running Pebble."
                            : unprocessed
                              ? "Saved inputs have changed. Run Pebble to update the results."
                              : connection && !connection.hasResults
                                ? "Add reports and Run Pebble to create the first result set."
                                : `Last completed ${formatTime(results.completedAt)} PT · ${formatRunDate(results.completedAt)}`}
              </span>
            </div>
            {connection?.officer && connection.queueError && (
              <button className="button" onClick={connection.retryQueue}>
                Retry run queue
              </button>
            )}
            {runButton}
          </div>
          {storageError && (
            <Notice tone="amber">
              Browser storage is unavailable. The demo works, but changes won’t
              survive a reload.
            </Notice>
          )}
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
          {page === "reports" && (
            <>
              <div className="stats-grid">
                <Stat
                  icon={<FileText size={20} />}
                  label="Included reports"
                  value={
                    <>
                      {included.length}
                      <span className="stat-denominator">
                        {" "}
                        / {inputs.reports.length}
                      </span>
                    </>
                  }
                  detail="Ready for your next run"
                />
                <Stat
                  icon={<Activity size={20} />}
                  label="Raid nights"
                  value={results.nights.length}
                  detail="In the latest completed results"
                />
                <Stat
                  icon={<Users size={20} />}
                  label={connection ? "Team roster" : "Sample roster"}
                  value={players.length}
                  detail="Players tracked this season"
                />
              </div>
              <ReportsPanel
                reports={inputs.reports}
                editor={reportEditor}
                demo={!connection}
              />
            </>
          )}
          {page === "overrides" && (
            <>
              <div className="override-toolbar">
                <label className="night-select">
                  <Clock3 size={16} />
                  <span>Raid night</span>
                  <select
                    aria-label="Raid night"
                    value={night}
                    onChange={(e) => chooseNight(e.target.value)}
                  >
                    {[...new Set([...nightOptions, night])]
                      .sort()
                      .reverse()
                      .map((n) => (
                        <option key={n} value={n}>
                          {formatDate(n, true)}
                        </option>
                      ))}
                  </select>
                </label>
                <label className="custom-date small muted">
                  Other date
                  <input
                    aria-label="Choose another raid date"
                    type="date"
                    value={night}
                    onInput={(e) =>
                      e.currentTarget.value &&
                      chooseNight(e.currentTarget.value)
                    }
                  />
                </label>
              </div>
              <OverrideTabs
                value={overrideTab}
                onChange={(next) => guard(() => setOverrideTab(next))}
              />
              {overrideTab === "availability" ? (
                <div
                  className="override-grid"
                  id="override-panel"
                  role="tabpanel"
                  aria-labelledby="override-tab-availability"
                >
                  <section className="card player-card">
                    <div className="table-heading">
                      <div>
                        <h2>Who was available?</h2>
                        <p>Select a player to adjust their availability.</p>
                      </div>
                    </div>
                    <label className="search-field">
                      <Search size={16} />
                      <input
                        aria-label="Search players"
                        placeholder="Find a player or class…"
                        value={search}
                        onChange={(e) => setSearch(e.target.value)}
                      />
                    </label>
                    <div className="player-list">
                      {visiblePlayers.map((player) => {
                        const a =
                          inputs.adjustments[adjustmentKey(night, player.id)];
                        const overridden =
                          a &&
                          (a.pre.mode !== "automatic" ||
                            a.post.mode !== "automatic");
                        const row = latestNight?.players.find(
                          (r) => r.id === player.id,
                        );
                        return (
                          <button
                            key={player.id}
                            className={`player-item ${selected === player.id ? "selected" : ""}`}
                            onClick={() => choosePlayer(player.id)}
                            aria-pressed={selected === player.id}
                          >
                            <span
                              className="class-mark"
                              style={{
                                color: player.color,
                                background: `${player.color}12`,
                              }}
                            >
                              {player.name.slice(0, 1)}
                            </span>
                            <span className="player-description">
                              <strong style={{ color: player.color }}>
                                {player.name}
                              </strong>
                              {playerDetails(player) && (
                                <span>{playerDetails(player)}</span>
                              )}
                            </span>
                            <span className="player-availability">
                              {overridden ? (
                                <Badge tone="purple">Override</Badge>
                              ) : (
                                <span className="muted small">Automatic</span>
                              )}
                              <span>
                                {row
                                  ? `${row.bench} min bench`
                                  : "Not calculated"}
                              </span>
                            </span>
                            <ChevronRight size={14} />
                          </button>
                        );
                      })}
                      {!visiblePlayers.length && (
                        <p className="empty-state" role="status">
                          {players.length
                            ? "No matching players. Try another search."
                            : "No players in the roster yet."}
                        </p>
                      )}
                    </div>
                    <div className="table-footnote">
                      <Info size={14} /> Bench totals are from the last
                      completed run.
                    </div>
                  </section>
                  <AvailabilityEditor
                    player={selectedPlayer}
                    editor={availabilityEditor}
                    span={span}
                  />
                </div>
              ) : (
                <div
                  id="override-panel"
                  role="tabpanel"
                  aria-labelledby="override-tab-timing"
                >
                  <TimingEditor
                    night={night}
                    detected={detectedTiming}
                    editor={timingEditor}
                  />
                </div>
              )}
            </>
          )}
          {page === "results" && (!connection || connection.hasResults) && (
            <>
              {Boolean(results.warnings?.length) && (
                <section
                  className="notice amber"
                  aria-labelledby="result-warnings-title"
                >
                  <Info size={16} aria-hidden="true" />
                  <div>
                    <h2 id="result-warnings-title">Result warnings</h2>
                    <p>From the displayed completed run.</p>
                    <ul>
                      {results.warnings?.map((warning, index) => (
                        <li key={index}>{warning}</li>
                      ))}
                    </ul>
                    {connection?.officer &&
                      connection.resultSources.length > 0 && (
                        <section aria-labelledby="warning-sources-title">
                          <h3 id="warning-sources-title">
                            Source reports for this run
                          </h3>
                          <ul>
                            {connection.resultSources.map((source) => (
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
                        </section>
                      )}
                  </div>
                </section>
              )}
              <div className="stats-grid">
                <Stat
                  icon={<Clock3 size={20} />}
                  label="Played together"
                  value={minutes(
                    results.players.reduce((s, r) => s + r.played, 0),
                  )}
                  detail="Total player time"
                />
                <Stat
                  icon={<Users size={20} />}
                  label="Bench time"
                  value={minutes(
                    results.players.reduce((s, r) => s + r.bench, 0),
                  )}
                  detail="Tracked across included raid nights"
                />
                <Stat
                  icon={<Activity size={20} />}
                  label="Raid nights"
                  value={results.nights.length}
                  detail={`${results.reportCount} included reports`}
                />
              </div>
              <section className="card results-card">
                <div className="table-heading">
                  <div>
                    <h2>Time on the team</h2>
                    <p>
                      Click a player for their pre- and post-break breakdown.
                    </p>
                  </div>
                  <select
                    aria-label="Result period"
                    value={visibleResultNight}
                    onChange={(e) => {
                      setResultNight(e.target.value);
                      setExpandedPlayer(null);
                    }}
                  >
                    <option value="season">Season to date</option>
                    {results.nights.map((n) => (
                      <option value={n.night} key={n.night}>
                        {formatDate(n.night, true)}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="table-scroll">
                  <table>
                    <thead>
                      <tr>
                        <th>#</th>
                        <th>Player</th>
                        <th>Played</th>
                        <th aria-sort="ascending">
                          Bench <ArrowDown size={12} />
                        </th>
                        <th>Time breakdown</th>
                        <th />
                      </tr>
                    </thead>
                    <tbody>
                      {resultRows.map((row, i) => {
                        const player = row.name
                          ? {
                              id: row.id,
                              name: row.name,
                              className: row.className ?? "",
                              role: row.role ?? "Damage",
                              color: row.color ?? "#b9c7d8",
                              pre: 0,
                              post: 0,
                            }
                          : players.find((p) => p.id === row.id)!;
                        return (
                          <ResultRow
                            key={row.id}
                            player={player}
                            row={row}
                            index={i}
                            expanded={expandedPlayer === row.id}
                            onExpand={() =>
                              setExpandedPlayer(
                                expandedPlayer === row.id ? null : row.id,
                              )
                            }
                            onEdit={
                              !canManage
                                ? undefined
                                : () => {
                                    guard(() => {
                                      const n =
                                        visibleResultNight === "season"
                                          ? (results.nights[0]?.night ??
                                            defaultNight)
                                          : visibleResultNight;
                                      nightChosen.current = true;
                                      setNight(n);
                                      setSelected(row.id);
                                      setOverrideTab("availability");
                                      setPage("overrides");
                                    });
                                  }
                            }
                          />
                        );
                      })}
                    </tbody>
                  </table>
                  {!resultRows.length && (
                    <div className="empty-state">
                      <FileText size={28} />
                      <h3>No player time for this period</h3>
                      <p>
                        {canManage
                          ? "Use Manage reports to review the included logs."
                          : "No player time was recorded for this period in the completed results."}
                        {results.nights.length > 0 &&
                          " Try another result period."}
                      </p>
                    </div>
                  )}
                </div>
                <div className="table-footnote">
                  <CheckCheck size={14} />{" "}
                  {connection && !connection.hasResults
                    ? "No completed result set loaded."
                    : `${connection && (connection.resultsPending || connection.resultsError) ? "Previous completed results" : "Latest completed run"} · ${formatTime(results.completedAt)} PT. Results update automatically after a successful run.`}
                </div>
              </section>
              {!connection && (
                <Notice>
                  These are illustrative sample calculations, not Pebble’s
                  production fairness engine. Notes and officer details are kept
                  out of the results view.
                </Notice>
              )}
            </>
          )}
          {canManage && (
            <details className="card run-history">
              <summary>
                <div>
                  <Clock3 size={16} />
                  <strong>Run history</strong>
                  <span className="muted small">
                    {history.length} completed{" "}
                    {history.length === 1 ? "run" : "runs"}
                  </span>
                </div>
                <ChevronDown size={16} />
              </summary>
              <div>
                {!history.length && (
                  <p className="empty-state">
                    No completed runs in recent history.
                  </p>
                )}
                {history.map((h) => (
                  <div className="history-row" key={h.id}>
                    {h.status === "succeeded" ? (
                      <Check size={16} className="green-text" />
                    ) : (
                      <CircleAlert size={16} className="red-text" />
                    )}
                    <div>
                      <strong>
                        {h.status === "succeeded"
                          ? connection
                            ? "Computation completed"
                            : "Results updated"
                          : "Run failed"}
                      </strong>
                      <p>{h.message}</p>
                    </div>
                    <span className="muted small">
                      {formatRunDate(h.at)} · {formatTime(h.at)} PT
                    </span>
                  </div>
                ))}
              </div>
            </details>
          )}
          <footer className="page-footer">
            <span>
              <span className="footer-pebble" /> Small tool. Fairer raid nights.
            </span>
            <span>
              {connection
                ? `${connection.label} · Pacific time`
                : "Local prototype · sample data only · no Firestore"}
            </span>
          </footer>
        </div>
      </main>
      {toast && (
        <div
          className={`toast ${toast.error ? "error" : ""}`}
          role={toast.error ? "alert" : "status"}
        >
          {toast.error ? <CircleAlert size={18} /> : <Check size={18} />}
          <span>{toast.text}</span>
          <button
            aria-label="Dismiss notification"
            onClick={() => setToast(null)}
          >
            <X size={16} />
          </button>
        </div>
      )}
      <ConfirmDiscard
        action={pendingAction}
        cancel={() => setPendingAction(null)}
        discard={() => {
          const action = pendingAction;
          clearEdits();
          setPendingAction(null);
          action?.();
        }}
      />
    </div>
  );
}
function ResultRow({
  player,
  row,
  index,
  expanded,
  onExpand,
  onEdit,
}: {
  player: (typeof samplePlayers)[number];
  row: ReturnType<typeof calculate>["players"][number];
  index: number;
  expanded: boolean;
  onExpand: () => void;
  onEdit?: () => void;
}) {
  const total = row.played + row.bench;
  return (
    <>
      <tr>
        <td className="muted rank">{String(index + 1).padStart(2, "0")}</td>
        <td>
          <button
            className="result-name"
            onClick={onExpand}
            aria-expanded={expanded}
          >
            <span className="class-dot" style={{ background: player.color }} />
            <span>
              <strong style={{ color: player.color }}>{player.name}</strong>
              <span className="cell-subtitle">{player.className}</span>
            </span>
          </button>
        </td>
        <td className="numeric">{minutes(row.played)}</td>
        <td className="numeric bench-value">{minutes(row.bench)}</td>
        <td>
          <div
            className="time-bar"
            aria-label={`${row.played} minutes played, ${row.bench} minutes benched`}
          >
            <span
              style={{ width: `${total ? (row.played / total) * 100 : 0}%` }}
            />
            <i />
          </div>
          <span className="small muted">
            {total ? Math.round((row.played / total) * 100) : 0}% played
            {row.overridden ? " · adjusted" : ""}
          </span>
        </td>
        <td>
          <button
            className="icon-button"
            aria-label={`Details for ${player.name}`}
            onClick={onExpand}
            aria-expanded={expanded}
          >
            {expanded ? <ChevronDown size={17} /> : <ChevronRight size={17} />}
          </button>
        </td>
      </tr>
      {expanded && (
        <tr className="result-detail">
          <td colSpan={6}>
            <div>
              <span>
                Before break bench <strong>{row.preBench} min</strong>
              </span>
              <span>
                After break bench <strong>{row.postBench} min</strong>
              </span>
              <span>
                Availability{" "}
                <strong>
                  {row.overridden
                    ? "Officer adjustment applied"
                    : "Automatically inferred"}
                </strong>
              </span>
              {onEdit && (
                <button className="text-button" onClick={onEdit}>
                  <Settings2 size={14} /> Edit availability{" "}
                  <ArrowRight size={14} />
                </button>
              )}
            </div>
          </td>
        </tr>
      )}
    </>
  );
}
