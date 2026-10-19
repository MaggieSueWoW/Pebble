import type { LiveConnection, Versions } from "./useFirestore";
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
  ExternalLink,
  FileText,
  FlaskConical,
  Info,
  LoaderCircle,
  Play,
  Plus,
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
  demoCode,
  emptyAdjustment,
  formatDate,
  formatTime,
  freshState,
  minutes,
  nightOptions as sampleNights,
  parseReports,
  players as samplePlayers,
  signature,
  stableJSON,
  timingSpans,
  validateAdjustment,
  wallTime,
  type Adjustment,
  type Availability,
  type Inputs,
  type Mode,
  type Page,
  type Saved,
  type Timing,
} from "./model";

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
const blankTiming = (): Timing => ({
  start: "",
  breakStart: "",
  breakEnd: "",
  end: "",
  notes: "",
});
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
function AvailabilityField({
  label,
  value,
  onChange,
  max,
}: {
  label: string;
  value: Availability;
  onChange: (a: Availability) => void;
  max: number;
}) {
  return (
    <div className="field">
      <label>
        {label}
        <select
          value={value.mode}
          onChange={(e) =>
            onChange(
              ["partial", "unavailableFor"].includes(e.target.value)
                ? { mode: e.target.value as Mode, minutes: max }
                : { mode: e.target.value as Mode },
            )
          }
        >
          <option value="automatic">Automatic</option>
          <option value="available">Available</option>
          <option value="unavailable">Unavailable</option>
          <option value="partial">Partial availability</option>
          <option value="unavailableFor">Unavailable for…</option>
        </select>
      </label>
      {["partial", "unavailableFor"].includes(value.mode) && (
        <label className="minutes-label">
          {value.mode === "unavailableFor"
            ? "Unavailable minutes"
            : "Available minutes"}{" "}
          <input
            type="number"
            min="1"
            max={value.mode === "unavailableFor" ? 720 : max}
            value={value.minutes ?? ""}
            onChange={(e) =>
              onChange({
                mode: value.mode,
                minutes:
                  e.target.value === "" ? undefined : Number(e.target.value),
              })
            }
          />
          <span className="muted small">
            {value.mode === "unavailableFor"
              ? "Pebble caps this adjustment to the raid half."
              : `Up to ${max} minutes in this half`}
          </span>
        </label>
      )}
    </div>
  );
}
export default function App({ connection }: { connection?: LiveConnection }) {
  const [demoSaved, setSaved] = useState<Saved>(
    () => connection?.saved ?? loadSaved(),
  );
  const saved = connection?.saved ?? demoSaved;
  const { inputs, results, history } = saved;
  const canManage = !connection || connection.officer;
  const players = connection?.players ?? samplePlayers;
  const today = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Los_Angeles",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date());
  const nightOptions = connection
    ? connection.nights.length
      ? connection.nights
      : [today]
    : sampleNights;
  const firstPlayer = players[0]?.id ?? "";
  const [page, setPage] = useState<Page>(canManage ? "reports" : "results");
  const [reportText, setReportText] = useState("");
  const [includeEdits, setIncludeEdits] = useState<Record<string, boolean>>({});
  const [night, setNight] = useState(nightOptions[0]);
  const [overrideTab, setOverrideTab] = useState<"availability" | "timing">(
    "availability",
  );
  const [selected, setSelected] = useState(firstPlayer);
  const [editVersions, setEditVersions] = useState<Versions>(
    connection?.versions ?? {},
  );
  const [timingVersions, setTimingVersions] = useState<Versions>(
    connection?.versions ?? {},
  );
  const inclusionVersions = useRef<Versions>({});
  const [edit, setEdit] = useState<Adjustment>(() =>
    structuredClone(
      inputs.adjustments[adjustmentKey(nightOptions[0], firstPlayer)] ??
        emptyAdjustment(),
    ),
  );
  const [timingEdit, setTimingEdit] = useState<Timing>(() =>
    structuredClone(inputs.timings[nightOptions[0]] ?? blankTiming()),
  );
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
  const lastAdjustment =
    inputs.adjustments[adjustmentKey(night, selected)] ?? emptyAdjustment();
  const editDirty = stableJSON(edit) !== stableJSON(lastAdjustment);
  const timingDirty =
    stableJSON(timingEdit) !==
    stableJSON(inputs.timings[night] ?? blankTiming());
  const unsaved =
    Boolean(reportText.trim()) ||
    Object.keys(includeEdits).length > 0 ||
    editDirty ||
    timingDirty;
  const unprocessed = connection
    ? connection.unprocessed
    : results.signature !== signature(inputs);
  const parsed = parseReports(reportText, inputs.reports);
  const ready = parsed.filter((p) => p.state === "ready");
  const included = inputs.reports.filter((r) => r.enabled);
  const visiblePlayers = players.filter((p) =>
    `${p.name} ${p.className}`.toLowerCase().includes(search.toLowerCase()),
  );
  const p = players.find((p) => p.id === selected) ?? {
    id: "",
    name: "Select a player",
    className: "",
    role: "",
    color: "#b9c7d8",
    pre: 0,
    post: 0,
  };
  const detected = (n: string): Timing =>
    connection
      ? (results.nights.find((item) => item.night === n)?.timing ??
        blankTiming())
      : defaultTiming(n);
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
  const adjustmentError = validateAdjustment(edit, span.pre, span.post);
  let timingError: string | null = null;
  try {
    if (connection) {
      const values = ["start", "breakStart", "breakEnd", "end"]
        .map(
          (k) =>
            timingEdit[k as keyof Timing] || detected(night)[k as keyof Timing],
        )
        .filter(Boolean);
      if (Boolean(timingEdit.breakStart) !== Boolean(timingEdit.breakEnd))
        throw new Error("Set both break boundaries or leave both automatic.");
      if (values.some((v, i) => i > 0 && v <= values[i - 1]))
        throw new Error(
          "Timing must follow start, break start, break end, end.",
        );
    } else timingSpans(night, timingEdit);
  } catch (e) {
    timingError = (e as Error).message;
  }

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

  async function updateInputs(next: Inputs, expected?: Versions) {
    try {
      if (connection) await connection.saveInputs(next, expected);
      else setSaved((old) => ({ ...old, inputs: next }));
      return true;
    } catch (error) {
      notify((error as Error).message, true);
      return false;
    }
  }
  function clearEdits() {
    setEditVersions(connection?.versions ?? {});
    setTimingVersions(connection?.versions ?? {});
    setReportText("");
    setIncludeEdits({});
    setEdit(structuredClone(lastAdjustment));
    setTimingEdit(structuredClone(inputs.timings[night] ?? blankTiming()));
  }
  function guard(action: () => void) {
    if (unsaved) setPendingAction(() => action);
    else action();
  }
  function go(next: Page) {
    if (!canManage && next !== "results") return;
    if (next !== page) guard(() => setPage(next));
  }
  function chooseNight(next: string) {
    guard(() => {
      setNight(next);
      setEditVersions(connection?.versions ?? {});
      setTimingVersions(connection?.versions ?? {});
      setEdit(
        structuredClone(
          inputs.adjustments[adjustmentKey(next, selected)] ??
            emptyAdjustment(),
        ),
      );
      setTimingEdit(structuredClone(inputs.timings[next] ?? blankTiming()));
    });
  }
  function choosePlayer(id: string) {
    if (id === selected) return;
    guard(() => {
      setSelected(id);
      setEditVersions(connection?.versions ?? {});
      setEdit(
        structuredClone(
          inputs.adjustments[adjustmentKey(night, id)] ?? emptyAdjustment(),
        ),
      );
    });
  }
  async function addReports() {
    if (!ready.length || parsed.some((p) => p.state === "invalid")) return;
    const newReports = ready.map((item) => ({
      code: item.code,
      enabled: true,
      night: connection ? "" : nightOptions[0],
      title: connection ? "" : "Sample · Additional perspective",
      fetched: false,
    }));
    if (
      !(await updateInputs({
        ...inputs,
        reports: [...newReports, ...inputs.reports],
      }))
    )
      return;
    setReportText("");
    notify(
      `Added ${ready.length} report${ready.length === 1 ? "" : "s"}. Run Pebble when you’re ready.`,
    );
  }
  async function saveInclusion() {
    if (
      !(await updateInputs(
        {
          ...inputs,
          reports: inputs.reports.map((r) => ({
            ...r,
            enabled: includeEdits[r.code] ?? r.enabled,
          })),
        },
        inclusionVersions.current,
      ))
    )
      return;
    setIncludeEdits({});
    notify("Report selection saved. Run Pebble to update results.");
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
  async function saveAdjustment() {
    if (adjustmentError || !selected) return;
    if (
      !(await updateInputs(
        {
          ...inputs,
          adjustments: {
            ...inputs.adjustments,
            [adjustmentKey(night, selected)]: structuredClone(edit),
          },
        },
        editVersions,
      ))
    )
      return;
    const path = `availabilityOverrides/${night}_${selected}`;
    setEditVersions({
      ...connection?.versions,
      [path]: (editVersions[path] ?? 0) + 1,
    });
    notify(`Saved ${p.name}’s availability. Run Pebble to update results.`);
  }
  async function saveTiming() {
    if (timingError) return;
    if (
      !(await updateInputs(
        {
          ...inputs,
          timings: { ...inputs.timings, [night]: structuredClone(timingEdit) },
        },
        timingVersions,
      ))
    )
      return;
    const path = `nightOverrides/${night}`;
    setTimingVersions({
      ...connection?.versions,
      [path]: (timingVersions[path] ?? 0) + 1,
    });
    notify("Timing saved. Run Pebble to update results.");
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
      onClick={() => connection?.signIn().catch((e) => notify(e.message, true))}
    >
      Officer sign-in
    </button>
  );
  const latestNight = results.nights.find((n) => n.night === night);
  const resultRows = (
    resultNight === "season"
      ? results.players
      : (results.nights.find((n) => n.night === resultNight)?.players ?? [])
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
                  setNight(nightOptions[0]);
                  setSelected("Example");
                  setEdit(emptyAdjustment());
                  setTimingEdit(blankTiming());
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
                onClick={() =>
                  guard(() => {
                    void connection.signOut();
                  })
                }
              >
                Sign out
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
          {connection?.user && !connection.officer && (
            <Notice>
              This account does not have officer access. Firebase UID:{" "}
              {connection.user.uid}
            </Notice>
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
                No results yet. An officer can add reports and Run Pebble.
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
              <button
                className="button secondary"
                disabled={!canManage}
                onClick={() => go("reports")}
              >
                <FileText size={16} /> Manage reports
              </button>
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
            className={`run-strip ${active || waitingForResults ? "is-running" : ""} ${(lastFailed || resultLoadFailed) && !active ? "has-error" : ""}`}
            role="status"
          >
            <div
              className={`strip-icon ${active || waitingForResults ? "running-icon" : ""}`}
            >
              {active || (waitingForResults && !resultLoadFailed) ? (
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
              <section className="card add-card">
                <div className="section-title">
                  <span className="icon-tile">
                    <Plus size={20} />
                  </span>
                  <div>
                    <h2>Add a report</h2>
                    <p>
                      Paste Warcraft Logs links or report codes, one per line.
                    </p>
                  </div>
                  <Badge tone="purple">WARCRAFT LOGS</Badge>
                </div>
                <label className="sr-only" htmlFor="report-input">
                  Warcraft Logs reports
                </label>
                <textarea
                  id="report-input"
                  rows={3}
                  value={reportText}
                  onChange={(e) => setReportText(e.target.value)}
                  placeholder={
                    "https://www.warcraftlogs.com/reports/…\nOr a 16-character report code"
                  }
                  spellCheck={false}
                  aria-describedby="report-help"
                />
                {parsed.length > 0 && (
                  <ul className="parse-results" aria-live="polite">
                    {parsed.map((item, i) => (
                      <li
                        key={`${item.raw}-${i}`}
                        className={item.state === "invalid" ? "invalid" : ""}
                      >
                        {item.state === "ready" ? (
                          <Check size={14} />
                        ) : item.state === "invalid" ? (
                          <CircleAlert size={14} />
                        ) : (
                          <Info size={14} />
                        )}
                        <code>{item.code || item.raw}</code>
                        <span>{item.message}</span>
                        {item.state === "disabled" && (
                          <button
                            className="text-button"
                            onClick={async () => {
                              if (
                                !(await updateInputs({
                                  ...inputs,
                                  reports: inputs.reports.map((r) =>
                                    r.code === item.code
                                      ? { ...r, enabled: true }
                                      : r,
                                  ),
                                }))
                              )
                                return;
                              setReportText((text) =>
                                text
                                  .split(/\r?\n/)
                                  .filter((line) => !line.includes(item.code))
                                  .join("\n"),
                              );
                              notify(
                                "Report included again. Run Pebble to update results.",
                              );
                            }}
                          >
                            Re-enable
                          </button>
                        )}
                      </li>
                    ))}
                  </ul>
                )}
                <div className="card-action-row">
                  <span className="muted small" id="report-help">
                    <ShieldCheck size={14} /> Adding a report won’t start the
                    bot.
                  </span>
                  <div className="button-row">
                    {!connection && (
                      <button
                        className="text-button"
                        onClick={() => {
                          setReportText(
                            (old) =>
                              `${old.trim() ? old.trim() + "\n" : ""}https://www.warcraftlogs.com/reports/${demoCode}`,
                          );
                        }}
                      >
                        Try a sample link <ArrowUpRight size={13} />
                      </button>
                    )}
                    <button
                      className="button primary compact"
                      onClick={addReports}
                      disabled={
                        !ready.length ||
                        parsed.some((p) => p.state === "invalid")
                      }
                    >
                      <Plus size={15} /> Add{" "}
                      {ready.length > 1 ? `${ready.length} reports` : "report"}
                    </button>
                  </div>
                </div>
              </section>
              <section className="card reports-card">
                <div className="table-heading">
                  <div>
                    <h2>
                      Your reports{" "}
                      <span className="count-pill">
                        {inputs.reports.length}
                      </span>
                    </h2>
                    <p>Include the logs you want Pebble to use.</p>
                  </div>
                  <span className="small muted">All dates in Pacific time</span>
                </div>
                <div className="table-scroll">
                  <table>
                    <thead>
                      <tr>
                        <th>Report</th>
                        <th>Raid night</th>
                        <th>Status</th>
                        <th className="align-right">Include</th>
                      </tr>
                    </thead>
                    <tbody>
                      {inputs.reports.map((r) => {
                        const enabled = includeEdits[r.code] ?? r.enabled;
                        return (
                          <tr
                            key={r.code}
                            className={enabled ? "" : "excluded-row"}
                          >
                            <td>
                              <div className="report-cell">
                                <span
                                  className={`report-icon ${!r.fetched ? "unfetched" : ""}`}
                                >
                                  <FileText size={18} />
                                </span>
                                <div>
                                  <a
                                    href={`https://www.warcraftlogs.com/reports/${r.code}`}
                                    target="_blank"
                                    rel="noreferrer"
                                  >
                                    {r.fetched ? r.title : r.code}
                                    <ExternalLink size={12} />
                                  </a>
                                  <span className="report-code">
                                    {r.fetched
                                      ? r.code
                                      : "Metadata appears after the next run"}
                                  </span>
                                </div>
                              </div>
                            </td>
                            <td>
                              {r.fetched ? (
                                <>
                                  <span className="cell-title">
                                    {formatDate(r.night)}
                                  </span>
                                  <span className="cell-subtitle">
                                    {new Intl.DateTimeFormat("en-US", {
                                      weekday: "long",
                                      timeZone: "UTC",
                                    }).format(new Date(`${r.night}T12:00:00Z`))}
                                  </span>
                                </>
                              ) : (
                                <span className="muted">—</span>
                              )}
                            </td>
                            <td>
                              <Badge
                                tone={
                                  !enabled
                                    ? "neutral"
                                    : !r.fetched
                                      ? "amber"
                                      : "green"
                                }
                              >
                                {!enabled
                                  ? "Excluded"
                                  : !r.fetched
                                    ? "Not fetched"
                                    : "Processed"}
                              </Badge>
                            </td>
                            <td className="align-right">
                              <button
                                className={`switch ${enabled ? "on" : ""}`}
                                role="switch"
                                aria-checked={enabled}
                                aria-label={`Include ${r.code}`}
                                onClick={() =>
                                  setIncludeEdits((old) => {
                                    if (!Object.keys(old).length)
                                      inclusionVersions.current = {
                                        ...connection?.versions,
                                      };
                                    const next = { ...old };
                                    if (!enabled === r.enabled)
                                      delete next[r.code];
                                    else next[r.code] = !enabled;
                                    return next;
                                  })
                                }
                              >
                                <span />
                              </button>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
                {Object.keys(includeEdits).length > 0 && (
                  <div className="inclusion-save">
                    <span className="small">
                      {Object.keys(includeEdits).length} unsaved selection
                      change{Object.keys(includeEdits).length === 1 ? "" : "s"}
                    </span>
                    <div className="button-row">
                      <button
                        className="button ghost compact"
                        onClick={() => setIncludeEdits({})}
                      >
                        Cancel
                      </button>
                      <button
                        className="button primary compact"
                        onClick={saveInclusion}
                      >
                        Save selection
                      </button>
                    </div>
                  </div>
                )}
                <div className="table-footnote">
                  <Info size={14} /> Excluded reports stay here. They won’t
                  contribute to the next run.
                </div>
              </section>
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
                    {[
                      ...new Set([
                        ...nightOptions,
                        ...inputs.reports.map((r) => r.night),
                        night,
                      ]),
                    ]
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
              <div className="tabs" role="tablist" aria-label="Override type">
                <button
                  role="tab"
                  aria-selected={overrideTab === "availability"}
                  className={overrideTab === "availability" ? "selected" : ""}
                  onClick={() => guard(() => setOverrideTab("availability"))}
                >
                  <Users size={16} /> Player availability
                </button>
                <button
                  role="tab"
                  aria-selected={overrideTab === "timing"}
                  className={overrideTab === "timing" ? "selected" : ""}
                  onClick={() => guard(() => setOverrideTab("timing"))}
                >
                  <Clock3 size={16} /> Raid timing
                </button>
              </div>
              {overrideTab === "availability" ? (
                <div className="override-grid">
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
                              <span>
                                {player.className} · {player.role}
                              </span>
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
                        <p className="empty-state">No matching players.</p>
                      )}
                    </div>
                    <div className="table-footnote">
                      <Info size={14} /> Bench totals are from the last
                      completed run.
                    </div>
                  </section>
                  <section className="card adjustment-card">
                    <div className="editor-heading">
                      <span
                        className="class-mark large"
                        style={{ color: p.color, background: `${p.color}12` }}
                      >
                        {p.name.slice(0, 1)}
                      </span>
                      <div>
                        <h2 style={{ color: p.color }}>{p.name}</h2>
                        <p>
                          {p.className} · {p.role}
                        </p>
                      </div>
                      {editDirty && <Badge tone="amber">Unsaved</Badge>}
                    </div>
                    <p className="editor-intro">
                      Tell Pebble when this player was available. Automatic uses
                      the bot’s normal inference.
                    </p>
                    <AvailabilityField
                      label="Before break"
                      value={edit.pre}
                      max={span.pre}
                      onChange={(pre) => setEdit((old) => ({ ...old, pre }))}
                    />
                    <AvailabilityField
                      label="After break"
                      value={edit.post}
                      max={span.post}
                      onChange={(post) => setEdit((old) => ({ ...old, post }))}
                    />
                    <div className="field">
                      <label>
                        Notes{" "}
                        <span className="optional">optional · private</span>
                        <textarea
                          rows={2}
                          maxLength={500}
                          placeholder="Anything helpful to remember…"
                          value={edit.notes}
                          onChange={(e) =>
                            setEdit((old) => ({
                              ...old,
                              notes: e.target.value,
                            }))
                          }
                        />
                      </label>
                    </div>
                    {adjustmentError && (
                      <p role="alert" className="field-error">
                        <CircleAlert size={14} />
                        {adjustmentError}
                      </p>
                    )}
                    <div className="editor-actions">
                      <button
                        className="button primary"
                        onClick={saveAdjustment}
                        disabled={
                          !editDirty ||
                          Boolean(adjustmentError) ||
                          !selected ||
                          Boolean(connection?.busy)
                        }
                      >
                        <Check size={15} /> Save adjustment
                      </button>
                      <button
                        className="button secondary"
                        disabled={!editDirty}
                        onClick={() => {
                          setEdit(structuredClone(lastAdjustment));
                          setEditVersions(connection?.versions ?? {});
                        }}
                      >
                        Cancel
                      </button>
                    </div>
                    <button
                      className="text-button reset-link"
                      onClick={() => setEdit(emptyAdjustment())}
                    >
                      <RotateCcw size={13} /> Reset to automatic
                    </button>
                    <Notice>
                      Save first, then Run Pebble. Current results stay the same
                      until the run completes.
                    </Notice>
                  </section>
                </div>
              ) : (
                <section className="card timing-card">
                  <div className="section-title">
                    <span className="icon-tile">
                      <Clock3 size={20} />
                    </span>
                    <div>
                      <h2>A little timing correction</h2>
                      <p>
                        Leave a field empty to use the detected time. All times
                        are Pacific.
                      </p>
                    </div>
                  </div>
                  <div className="timeline">
                    <span>
                      Raid start
                      <strong>{wallTime(detected(night).start)}</strong>
                    </span>
                    <div className="timeline-track">
                      <i />
                      <b />
                      <i />
                    </div>
                    <span>
                      Raid end
                      <strong>{wallTime(detected(night).end)}</strong>
                    </span>
                  </div>
                  <div className="timing-fields">
                    {(
                      [
                        { key: "start", label: "Mythic start" },
                        { key: "breakStart", label: "Break start" },
                        { key: "breakEnd", label: "Break end" },
                        { key: "end", label: "Mythic end" },
                      ] as const
                    ).map((item) => (
                      <div className="field" key={item.key}>
                        <label>
                          {item.label}
                          <input
                            type="datetime-local"
                            value={timingEdit[item.key]}
                            onInput={(e) => {
                              const value = e.currentTarget.value;
                              setTimingEdit((old) => ({
                                ...old,
                                [item.key]: value,
                              }));
                            }}
                          />
                        </label>
                        <span className="muted small">
                          Detected: {formatDate(night)} ·{" "}
                          {wallTime(detected(night)[item.key])}
                        </span>
                      </div>
                    ))}
                  </div>
                  <div className="field">
                    <label>
                      Notes <span className="optional">optional · private</span>
                      <textarea
                        rows={2}
                        maxLength={500}
                        placeholder="Anything useful about this night…"
                        value={timingEdit.notes}
                        onChange={(e) =>
                          setTimingEdit((old) => ({
                            ...old,
                            notes: e.target.value,
                          }))
                        }
                      />
                    </label>
                  </div>
                  {timingError && (
                    <p role="alert" className="field-error">
                      <CircleAlert size={14} />
                      {timingError}
                    </p>
                  )}
                  <div className="card-action-row">
                    <button
                      className="text-button"
                      onClick={() => setTimingEdit(blankTiming())}
                    >
                      <RotateCcw size={14} /> Reset to automatic
                    </button>
                    <div className="button-row">
                      <button
                        className="button secondary"
                        disabled={!timingDirty}
                        onClick={() => {
                          setTimingVersions(connection?.versions ?? {});
                          setTimingEdit(
                            structuredClone(
                              inputs.timings[night] ?? blankTiming(),
                            ),
                          );
                        }}
                      >
                        Cancel
                      </button>
                      <button
                        className="button primary"
                        disabled={
                          !timingDirty ||
                          Boolean(timingError) ||
                          Boolean(connection?.busy)
                        }
                        onClick={saveTiming}
                      >
                        <Check size={15} /> Save timing
                      </button>
                    </div>
                  </div>
                </section>
              )}
            </>
          )}
          {page === "results" && (!connection || connection.hasResults) && (
            <>
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
                    value={
                      results.nights.some((n) => n.night === resultNight)
                        ? resultNight
                        : "season"
                    }
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
                                        resultNight === "season"
                                          ? (results.nights[0]?.night ??
                                            nightOptions[0])
                                          : resultNight;
                                      setNight(n);
                                      setSelected(row.id);
                                      setEditVersions(
                                        connection?.versions ?? {},
                                      );
                                      setTimingVersions(
                                        connection?.versions ?? {},
                                      );
                                      setEdit(
                                        structuredClone(
                                          inputs.adjustments[
                                            adjustmentKey(n, row.id)
                                          ] ?? emptyAdjustment(),
                                        ),
                                      );
                                      setTimingEdit(
                                        structuredClone(
                                          inputs.timings[n] ?? blankTiming(),
                                        ),
                                      );
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
                      <h3>No included raid data</h3>
                      <p>
                        Include a report and Run Pebble to see results here.
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
