import { authMessage } from "./authMessages";
import type { LiveConnection } from "./useFirestore";
import { useEffect, useRef, useState, type ReactNode } from "react";
import {
  ArrowDown,
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
  Link2,
  Play,
  RotateCcw,
  Search,
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
  nightOptions as sampleNights,
  players as samplePlayers,
  signature,
  timingSpans,
  type Inputs,
  type Page,
  type Saved,
} from "./model";

import { resultLink, useResultNavigation } from "./useResultNavigation";
import { NightDiagnostics } from "./NightDiagnostics";
import { orderResultRows } from "./resultRanking";
import { ResultRow } from "./ResultRow";
import { AttendanceDetails } from "./AttendanceDetails";
import { useReportEditor } from "./useReportEditor";
import { ReportsPanel } from "./ReportsPanel";
import { AvailabilityEditor } from "./AvailabilityEditor";
import { useAvailabilityEditor } from "./useAvailabilityEditor";
import { TimingEditor } from "./TimingEditor";
import { OverrideTabs } from "./OverrideTabs";
import { useTimingEditor } from "./useTimingEditor";
import { CharacterMappingsPanel } from "./CharacterMappingsPanel";
import { useCharacterEditor } from "./useCharacterEditor";
import { CalculationEditor } from "./CalculationEditor";
import { useCalculationEditor } from "./useCalculationEditor";
import { defaultCalculation } from "./calculation";
import { RosterList } from "./RosterList";
import { RosterPanel } from "./RosterPanel";
import { useRosterEditor } from "./useRosterEditor";
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
      aria-labelledby="discard-title"
      aria-describedby="discard-description"
      onCancel={(e) => {
        e.preventDefault();
        cancel();
      }}
    >
      <h2 id="discard-title">Keep your changes?</h2>
      <p id="discard-description">
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
  const nightOptions = connection
    ? [
        ...new Set(
          [
            ...connection.nights,
            ...inputs.reports
              .filter((report) => report.fetched)
              .map((report) => report.night),
          ].filter(Boolean),
        ),
      ]
        .sort()
        .reverse()
    : sampleNights;
  const defaultNight = nightOptions[0] ?? "";
  const firstPlayer =
    [...players].sort((a, b) =>
      a.name.localeCompare(b.name, "en", { sensitivity: "base" }),
    )[0]?.id ?? "";
  const navigation = useResultNavigation(
    canManage ? "reports" : "results",
    () => unsaved,
    (action) => guard(action),
  );
  const page = navigation.page;
  const showPageIntro = page === "settings";
  const compactRunStatus = page === "reports" || page === "roster";
  const setPage = (page: Page) => navigation.navigate({ page });
  const [night, setNight] = useState(defaultNight);
  const nightChosen = useRef(false);
  const [overrideTab, setOverrideTab] = useState<"availability" | "timing">(
    "availability",
  );
  const [selected, setSelected] = useState(firstPlayer);
  const [search, setSearch] = useState("");
  const resultNight = navigation.period;
  const expandedPlayer = navigation.player;
  const setResultNight = (period: string) =>
    navigation.navigate({ period, player: null });
  const setExpandedPlayer = (player: string | null) =>
    navigation.navigate({ player });
  const [resultLinkNotice, setResultLinkNotice] = useState<{
    text: string;
    period: string;
    player: string | null;
  } | null>(null);
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
      if (!nightOptions.includes(key.slice(0, slash)))
        throw new Error("Choose a raid night identified from logs.");
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
    Boolean(connection?.busy) || !nightOptions.includes(night),
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
      if (!nightOptions.includes(key))
        throw new Error("Choose a raid night identified from logs.");
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
    Boolean(connection?.busy) || !nightOptions.includes(night),
    !connection,
  );
  const [rosterDetail, setRosterDetail] = useState<"main" | "alt" | null>(null);
  const rosterEditor = useRosterEditor(
    connection?.roster ?? [],
    async (id, value, expected) => {
      if (!connection)
        throw new Error("Roster editing requires an officer connection.");
      return connection.savePlayer(id, value, expected);
    },
    Boolean(connection?.busy),
    Boolean(connection?.officer),
  );
  const characterEditor = useCharacterEditor(
    connection?.characters ?? [],
    connection?.roster ?? [],
    async (id, value, expected) => {
      if (!connection)
        throw new Error("Character mappings require an officer connection.");
      return connection.saveCharacter(id, value, expected);
    },
    Boolean(connection?.busy),
    Boolean(connection?.officer),
  );
  const calculationEditor = useCalculationEditor(
    connection?.calculation ?? defaultCalculation(),
    connection?.versions["settings/calculation"] ?? 0,
    async (value, expected) => {
      if (!connection)
        throw new Error("Calculation settings require an officer connection.");
      return connection.saveCalculation(value, expected);
    },
    Boolean(connection?.busy),
    Boolean(connection?.officer),
  );
  const unsaved =
    characterEditor.dirty ||
    characterEditor.saving ||
    calculationEditor.dirty ||
    calculationEditor.saving ||
    rosterEditor.dirty ||
    rosterEditor.saving ||
    reportEditor.dirty ||
    reportEditor.saving ||
    availabilityEditor.dirty ||
    availabilityEditor.saving ||
    timingEditor.dirty ||
    timingEditor.saving;
  useEffect(() => {
    // Follow arriving metadata only until the operator chooses or edits a date.
    if (unsaved) nightChosen.current = true;
    if (!unsaved && (!nightChosen.current || !nightOptions.includes(night)))
      setNight(defaultNight);
  }, [defaultNight, nightOptions.join(","), night, unsaved]);
  const unprocessed = connection
    ? connection.unprocessed
    : results.signature !== signature(inputs);
  const visiblePlayers = players
    .filter((p) =>
      `${p.name} ${p.className}`.toLowerCase().includes(search.toLowerCase()),
    )
    .sort((a, b) =>
      a.name.localeCompare(b.name, "en", { sensitivity: "base" }),
    );

  function retryWithFocus(retry: () => void) {
    retry();
    setTimeout(() => {
      if (document.activeElement === document.body)
        document.getElementById("page-title")?.focus({ preventScroll: true });
    }, 0);
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

  function clearEdits() {
    reportEditor.reset();
    availabilityEditor.cancel();
    timingEditor.cancel();
    rosterEditor.cancel();
    characterEditor.cancel();
    calculationEditor.cancel();
  }
  function guard(action: () => void) {
    if (
      characterEditor.saving ||
      calculationEditor.saving ||
      rosterEditor.saving ||
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
    if (!nightOptions.includes(next)) return;
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
  ) : null;
  const hasResultPeriod = (period: string) =>
    results.nights.some((n) => n.night === period);
  const visibleResultNight = hasResultPeriod(resultNight)
    ? resultNight
    : "season";
  const latestNight = results.nights.find((n) => n.night === night);
  const resultRows = orderResultRows(
    visibleResultNight === "season"
      ? results.players
      : (results.nights.find((n) => n.night === visibleResultNight)?.players ??
          []),
    visibleResultNight === "season" ? "season" : "night",
    (row) => row.name ?? players.find((p) => p.id === row.id)?.name ?? row.id,
  );
  const additionalAttendance =
    visibleResultNight === "season"
      ? results.players.filter(
          (row) =>
            row.attendance &&
            !resultRows.some((ranked) => ranked.id === row.id),
        )
      : [];
  const resultSelectionReady =
    !connection?.resultsPending && !connection?.resultsError;
  useEffect(() => {
    if (page !== "results" || !resultSelectionReady) return;
    if (resultNight !== "season" && !hasResultPeriod(resultNight)) {
      setResultLinkNotice({
        text: "This result period is not available in the latest completed results. Showing season totals.",
        period: "season",
        player: null,
      });
      navigation.navigate({ period: "season", player: null }, true);
    } else if (
      expandedPlayer &&
      !resultRows.some((row) => row.id === expandedPlayer) &&
      !additionalAttendance.some((row) => row.id === expandedPlayer)
    ) {
      setResultLinkNotice({
        text: "This player is not available for this result period. Showing all players.",
        period: resultNight,
        player: null,
      });
      navigation.navigate({ player: null }, true);
    }
  }, [page, results, resultNight, expandedPlayer, resultSelectionReady]);
  async function copyResultLink() {
    try {
      await navigator.clipboard.writeText(
        resultLink(
          { period: visibleResultNight, player: expandedPlayer },
          !connection,
        ),
      );
      notify("Result link copied.");
    } catch {
      notify(
        "Couldn’t copy the link. Copy the address from your browser.",
        true,
      );
    }
  }
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
                  <span className="nav-count">{inputs.reports.length}</span>
                ) : page === item.id ? (
                  <ChevronRight size={15} />
                ) : null}
              </button>
            ))}
        </nav>
        {canManage && <div className="sidebar-run">{runButton}</div>}
        <div className="sidebar-footer">
          {canManage && (
            <span className={`connection ${online ? "" : "offline"}`}>
              {online ? <Wifi size={14} /> : <WifiOff size={14} />}
              {online ? "Local bot online" : "Local bot offline"}
            </span>
          )}
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
      <main>
        <div
          className={`content ${page === "overrides" ? "overrides-page" : page === "roster" ? "roster-page" : ""}`}
        >
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
                    require an enabled officer account. Sign out and sign back
                    in to use another account.
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
              <button
                className="text-button"
                onClick={() => retryWithFocus(connection.retryResults)}
              >
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
              {showPageIntro && (
                <div className="eyebrow accent">A LITTLE LESS RAID ADMIN</div>
              )}
              <h1 id="page-title" tabIndex={-1}>
                {page === "reports"
                  ? "Reports"
                  : page === "overrides"
                    ? "Overrides"
                    : page === "settings"
                      ? "Calculation settings"
                      : page === "roster"
                        ? "Roster"
                        : "Results"}
              </h1>
              {showPageIntro && (
                <p>
                  {page === "settings"
                    ? "Set the calculation parameters, then recompute when ready."
                    : "Keep membership current, then recompute when ready."}
                </p>
              )}
            </div>
            {showPageIntro && (
              <button
                className="button secondary"
                onClick={() => go("results")}
              >
                View results <ArrowUpRight size={16} />
              </button>
            )}
          </div>
          {page !== "results" && page !== "overrides" && (
            <div
              className={`${compactRunStatus ? "reports-status" : "run-strip"} ${active || connection?.submitting || waitingForResults ? "is-running" : ""} ${(lastFailed || resultLoadFailed) && !active && !connection?.submitting ? "has-error" : ""}`}
            >
              {!compactRunStatus && (
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
              )}
              <div
                className="strip-copy"
                role="status"
                aria-label="Run status"
                aria-atomic="true"
              >
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
                                    : compactRunStatus
                                      ? null
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
                <button
                  className="button"
                  onClick={() => retryWithFocus(connection.retryQueue)}
                >
                  Retry run queue
                </button>
              )}
            </div>
          )}
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
              <ReportsPanel
                reports={inputs.reports}
                editor={reportEditor}
                demo={!connection}
              />
            </>
          )}
          {page === "settings" && connection?.officer && (
            <CalculationEditor editor={calculationEditor} />
          )}
          {page === "roster" && connection?.officer && (
            <>
              <RosterList
                close={() => guard(() => setRosterDetail(null))}
                roster={connection.roster}
                characters={connection.characters}
                colors={Object.fromEntries(
                  players.map((player) => [player.id, player.color]),
                )}
                editMain={(id) => {
                  if (rosterDetail !== "main" || rosterEditor.id !== id)
                    guard(() => {
                      rosterEditor.select(id);
                      setRosterDetail("main");
                    });
                }}
                addMain={() =>
                  guard(() => {
                    rosterEditor.add();
                    setRosterDetail("main");
                  })
                }
                editAlt={(id) => {
                  if (rosterDetail !== "alt" || characterEditor.id !== id)
                    guard(() => {
                      const alt = connection.characters.find(
                        (alt) => alt.id === id,
                      );
                      if (alt) rosterEditor.select(alt.playerId);
                      characterEditor.select(id);
                      setRosterDetail("alt");
                    });
                }}
                detailPlayerId={
                  rosterDetail && rosterEditor.exists
                    ? rosterEditor.id
                    : undefined
                }
                detail={
                  rosterDetail ? (
                    <div className="merged-roster-details">
                      <RosterPanel
                        editor={rosterEditor}
                        cancelCreate={() => setRosterDetail(null)}
                        deletionBlocked={
                          characterEditor.dirty || characterEditor.saving
                        }
                        linkedAlts={
                          connection.characters.filter(
                            (alt) => alt.playerId === rosterEditor.id,
                          ).length
                        }
                        linkedOverrides={
                          Object.keys(inputs.adjustments).filter(
                            (key) =>
                              key.slice(key.indexOf("/") + 1) ===
                              rosterEditor.id,
                          ).length
                        }
                        remove={async () => {
                          const expected = Object.fromEntries(
                            Object.entries(connection.versions).filter(
                              ([path]) =>
                                path === `players/${rosterEditor.id}` ||
                                (path.startsWith("characters/") &&
                                  connection.characters.some(
                                    (alt) =>
                                      alt.id === path.slice(11) &&
                                      alt.playerId === rosterEditor.id,
                                  )) ||
                                (path.startsWith("availabilityOverrides/") &&
                                  path.slice(
                                    "availabilityOverrides/".length + 11,
                                  ) === rosterEditor.id),
                            ),
                          );
                          await connection.deletePlayer(
                            rosterEditor.id,
                            expected,
                          );
                          setRosterDetail(null);
                        }}
                      />
                      {rosterEditor.exists && (
                        <CharacterMappingsPanel
                          characters={connection.characters}
                          mainName={
                            connection.roster.find(
                              (player) => player.id === rosterEditor.id,
                            )?.name ?? rosterEditor.value.name
                          }
                          mainClass={rosterEditor.value.className}
                          actionsBlocked={
                            rosterEditor.dirty || rosterEditor.saving
                          }
                          swap={async (id, version) => {
                            const main = connection.roster.find(
                              (player) => player.id === rosterEditor.id,
                            );
                            if (!main)
                              throw new Error(
                                "This main is no longer in the roster.",
                              );
                            await connection.swapMain(
                              main.id,
                              id,
                              main.version,
                              version,
                            );
                            setRosterDetail("main");
                          }}
                          playerId={rosterEditor.id}
                          editor={characterEditor}
                          editing={rosterDetail === "alt"}
                          cancelAdd={() => setRosterDetail("main")}
                          edit={(id) => {
                            if (
                              !characterEditor.dirty &&
                              !characterEditor.saving
                            ) {
                              characterEditor.select(id);
                              setRosterDetail("alt");
                            } else
                              guard(() => {
                                characterEditor.select(id);
                                setRosterDetail("alt");
                              });
                          }}
                          add={() => {
                            const action = () => {
                              characterEditor.add(rosterEditor.id);
                              setRosterDetail("alt");
                            };
                            if (
                              !characterEditor.dirty &&
                              !characterEditor.saving
                            )
                              action();
                            else guard(action);
                          }}
                          remove={async (id, version) => {
                            await connection.deleteCharacter(id, version);
                            if (characterEditor.id === id)
                              setRosterDetail("main");
                          }}
                        />
                      )}
                    </div>
                  ) : null
                }
              />
            </>
          )}
          {page === "overrides" && !nightOptions.length && (
            <Notice>
              No raid nights identified yet. Add reports and run Pebble to
              identify nights before adding overrides.
            </Notice>
          )}
          {page === "overrides" && nightOptions.length > 0 && (
            <>
              <div className="override-toolbar">
                <label className="night-select">
                  <Clock3 size={16} />
                  <span>Raid night</span>
                  <span className="select-control">
                    <select
                      aria-label="Raid night"
                      value={night}
                      onChange={(e) => chooseNight(e.target.value)}
                    >
                      {nightOptions.map((n) => (
                        <option key={n} value={n}>
                          {formatDate(n, true)}
                        </option>
                      ))}
                    </select>
                    <ChevronDown size={16} aria-hidden="true" />
                  </span>
                </label>
                <span
                  className="result-updated"
                  role="status"
                  aria-label="Run status"
                  aria-atomic="true"
                >
                  {active
                    ? `${active.status === "queued" && !online ? "Queued · waiting for your local bot" : `${steps[active.stage]}…`} · `
                    : connection?.submitting
                      ? "Submitting run… · "
                      : connection?.officer && !connection.queueReady
                        ? `${connection.queueError ? "Run queue is unavailable" : "Checking run queue…"} · `
                        : connection?.resultsError
                          ? "Latest results are unavailable · "
                          : connection?.resultsPending
                            ? "Loading latest results… · "
                            : lastFailed
                              ? "Last run didn’t finish · "
                              : unsaved
                                ? "You have unsaved edits · "
                                : unprocessed
                                  ? "Changes ready for the next run · "
                                  : ""}
                  {!connection || connection.hasResults ? (
                    <>
                      Updated{" "}
                      <time dateTime={results.completedAt}>
                        {formatTime(results.completedAt)} PT ·{" "}
                        {formatRunDate(results.completedAt)}
                      </time>
                    </>
                  ) : (
                    "No completed runs yet"
                  )}
                </span>
                {connection?.officer && connection.queueError && (
                  <button
                    className="text-button"
                    onClick={() => retryWithFocus(connection.retryQueue)}
                  >
                    Retry run queue
                  </button>
                )}
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
                      <div
                        className="override-player-columns"
                        aria-hidden="true"
                      >
                        <span>Player</span>
                        <span>
                          Bench<small>Before · After</small>
                        </span>
                        <span>
                          Played<small>Before · After</small>
                        </span>
                      </div>
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
                        const preOverride =
                          (row?.availability?.pre.mode ??
                            a?.pre.mode ??
                            "automatic") !== "automatic";
                        const postOverride =
                          (row?.availability?.post.mode ??
                            a?.post.mode ??
                            "automatic") !== "automatic";
                        return (
                          <button
                            key={player.id}
                            className={`player-item override-player-row ${selected === player.id ? "selected" : ""}`}
                            onClick={() => choosePlayer(player.id)}
                            aria-pressed={selected === player.id}
                          >
                            <span className="player-description">
                              <strong style={{ color: player.color }}>
                                {player.name}
                              </strong>
                            </span>
                            {(overridden || row?.overridden) && (
                              <Badge tone="purple">Override</Badge>
                            )}
                            {(
                              [
                                [
                                  "Bench before break",
                                  row?.preBench,
                                  preOverride,
                                ],
                                [
                                  "Bench after break",
                                  row?.postBench,
                                  postOverride,
                                ],
                                [
                                  "Played before break",
                                  row?.prePlayed,
                                  preOverride,
                                ],
                                [
                                  "Played after break",
                                  row?.postPlayed,
                                  postOverride,
                                ],
                              ] as const
                            ).map(([label, minutes, affected], index) => (
                              <span
                                key={label}
                                style={{ gridColumn: index + 3 }}
                                className={`override-minute ${affected ? "override-affected" : ""}`}
                                aria-label={`${label}: ${minutes == null ? "not recorded" : `${minutes} minutes`}${affected ? ", availability override" : ""}`}
                                title={`${label}${affected ? " · Availability override" : ""}`}
                              >
                                {minutes == null
                                  ? "—"
                                  : minutes === 0
                                    ? "-"
                                    : minutes}
                              </span>
                            ))}
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
                      <Info size={14} /> Minutes from the last completed run.
                      Highlighted values have an availability override.
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
          {page === "results" &&
            resultLinkNotice &&
            resultLinkNotice.period === resultNight &&
            resultLinkNotice.player === expandedPlayer && (
              <p role="status">{resultLinkNotice.text}</p>
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
              <section className="card results-card">
                <div className="results-toolbar">
                  <div className="result-period-select">
                    <select
                      aria-label="Result period"
                      value={visibleResultNight}
                      onChange={(e) => {
                        setResultLinkNotice(null);
                        setResultNight(e.target.value);
                      }}
                    >
                      <option value="season">Season to date</option>
                      {results.nights.map((n) => (
                        <option value={n.night} key={n.night}>
                          {formatDate(n.night, true)}
                        </option>
                      ))}
                    </select>
                    <ChevronDown size={16} aria-hidden="true" />
                  </div>
                  <span className="result-night-count">
                    {results.nights.length} raid{" "}
                    {results.nights.length === 1 ? "night" : "nights"} this
                    season
                  </span>
                  <span
                    className="result-updated"
                    role="status"
                    aria-label="Run status"
                    aria-atomic="true"
                  >
                    {connection?.resultsPending
                      ? "Loading latest results… · "
                      : connection?.resultsError
                        ? "Latest results are unavailable · "
                        : active
                          ? `${steps[active.stage]}… · `
                          : unprocessed
                            ? "Changes ready for the next run · "
                            : ""}
                    Updated{" "}
                    <time dateTime={results.completedAt}>
                      {formatTime(results.completedAt)} PT ·{" "}
                      {formatRunDate(results.completedAt)}
                    </time>
                  </span>
                  <button
                    className="text-button"
                    onClick={() => void copyResultLink()}
                    disabled={!resultSelectionReady}
                  >
                    <Link2 size={14} /> Copy result link
                  </button>
                </div>
                {canManage && connection && visibleResultNight !== "season" && (
                  <NightDiagnostics
                    night={visibleResultNight}
                    diagnostic={
                      connection.resultDiagnostics[visibleResultNight]
                    }
                  />
                )}
                <div className="table-scroll">
                  <table>
                    <thead>
                      <tr>
                        <th>#</th>
                        <th>Player</th>
                        <th aria-sort="ascending">
                          Bench <ArrowDown size={12} />
                        </th>
                        <th>Played</th>
                        <th>Time Played</th>
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
                              role: row.role ?? "",
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
                            showAttendance={visibleResultNight === "season"}
                            index={i}
                            rank={
                              visibleResultNight === "season"
                                ? row.ranking?.rank
                                : undefined
                            }
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
                          ? "Use Reports in the sidebar to review the included logs."
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
                    : "Results update automatically after a successful run."}
                </div>
              </section>
              {additionalAttendance.length > 0 && (
                <section
                  className="card additional-attendance"
                  aria-labelledby="additional-attendance-title"
                >
                  <h2 id="additional-attendance-title">
                    Additional attendance
                  </h2>
                  <p>
                    These published attendance records are separate from the
                    bench ranking.
                  </p>
                  {additionalAttendance.map((row) => (
                    <details key={row.id} open={expandedPlayer === row.id}>
                      <summary
                        onClick={(event) => {
                          event.preventDefault();
                          setExpandedPlayer(
                            expandedPlayer === row.id ? null : row.id,
                          );
                        }}
                      >
                        Attendance for {row.name ?? row.id}
                      </summary>
                      <AttendanceDetails attendance={row.attendance} />
                    </details>
                  ))}
                </section>
              )}
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
          {showPageIntro && (
            <footer className="page-footer">
              <span>
                <span className="footer-pebble" /> Small tool. Fairer raid
                nights.
              </span>
              <span>
                {connection
                  ? `${connection.label} · Pacific time`
                  : "Local prototype · sample data only · no Firestore"}
              </span>
            </footer>
          )}
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
