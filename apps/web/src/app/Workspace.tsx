import { Check, CircleAlert, LoaderCircle, Play, X } from "lucide-react";
import { useEffect, useRef, useState, type ReactNode } from "react";
import { authMessage } from "../authMessages";
import { adjustmentKey, emptyAdjustment, type Page } from "../model";
import type { LiveConnection } from "../useFirestore";

import { RunStatus } from "../RunStatus";
import { useReportEditor } from "../useReportEditor";
import { useResultNavigation } from "../useResultNavigation";

import { useAvailabilityEditor } from "../useAvailabilityEditor";
import { useCharacterEditor } from "../useCharacterEditor";
import { useTimingEditor } from "../useTimingEditor";

import { defaultCalculation } from "../calculation";
import { blankTiming } from "../timing";
import { useCalculationEditor } from "../useCalculationEditor";
import { useRosterEditor } from "../useRosterEditor";

import type { DemoSession } from "../demo/useDemoSession";
import { OverridesPage, type OverrideSort } from "../pages/OverridesPage";
import { ReportsPage } from "../pages/ReportsPage";
import { ResultsPage } from "../pages/ResultsPage";
import { RosterPage } from "../pages/RosterPage";
import { SettingsPage } from "../pages/SettingsPage";
import { useResultsView } from "../pages/useResultsView";
import { AppShell } from "./AppShell";
import { ConnectionNotices } from "./ConnectionNotices";
import { ConfirmDiscard, Notice } from "./Feedback";
import { RunHistory } from "./RunHistory";
import { steps } from "./runStages";
import { Sidebar } from "./Sidebar";
export function Workspace({
  connection,
  demo,
  renderDemoControls,
}:
  | { connection: LiveConnection; demo?: never; renderDemoControls?: never }
  | {
      connection?: never;
      demo: DemoSession;
      renderDemoControls: (onReset: () => void) => ReactNode;
    }) {
  const saved = connection?.saved ?? demo!.saved;
  const saveInputs = connection?.saveInputs ?? demo!.saveInputs;
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
  const players = connection?.players ?? demo!.players;
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
    : demo!.nights;
  const defaultNight = nightOptions[0] ?? "";
  const firstPlayer =
    [...players].sort((a, b) =>
      a.name.localeCompare(b.name, "en", { sensitivity: "base" }),
    )[0]?.id ?? "";
  const navigation = useResultNavigation(
    canManage ? "reports" : "results",
    () => unsaved || Boolean(connection?.busy),
    (action) => guard(action),
  );
  const page = navigation.page;
  const [overrideSort, setOverrideSort] = useState<OverrideSort>({
    key: "name",
    direction: "ascending",
  });
  function changeOverrideSort(key: typeof overrideSort.key) {
    setOverrideSort((current) => ({
      key,
      direction:
        current.key === key
          ? current.direction === "ascending"
            ? "descending"
            : "ascending"
          : key === "prePlayed" || key === "postPlayed"
            ? "descending"
            : "ascending",
    }));
  }
  const setPage = (page: Page) => navigation.navigate({ page });
  const [night, setNight] = useState(defaultNight);
  const nightChosen = useRef(false);
  const [overrideTab, setOverrideTab] = useState<"availability" | "timing">(
    "availability",
  );
  const [selected, setSelected] = useState(firstPlayer);
  const [search, setSearch] = useState("");
  const online = connection?.online ?? demo!.online;
  const active = connection ? connection.active : demo!.active;
  const [toast, setToast] = useState<{ text: string; error?: boolean } | null>(
    null,
  );
  const [pendingAction, setPendingAction] = useState<(() => void) | null>(null);
  const toastTimer = useRef<ReturnType<typeof setTimeout> | undefined>(
    undefined,
  );
  const reportEditor = useReportEditor(
    inputs.reports,
    connection?.versions ?? {},
    async (reports, expected) => {
      await saveInputs({ reports, adjustments: {}, timings: {} }, expected);
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
      : demo!.timingSpans(night, inputs.timings[night]);
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
      const acknowledged = await saveInputs(
        { reports: [], adjustments: { [key]: adjustment }, timings: {} },
        { [path]: expected },
      );
      return acknowledged[path] ?? 0;
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
    : demo!.detectedTiming(night);
  const timingEditor = useTimingEditor(
    night,
    inputs.timings[night] ?? blankTiming(),
    connection?.versions[`nightOverrides/${night}`] ?? 0,
    detectedTiming,
    async (key, timing, expected) => {
      if (!nightOptions.includes(key))
        throw new Error("Choose a raid night identified from logs.");
      const versions = await saveInputs(
        { reports: [], adjustments: {}, timings: { [key]: timing } },
        { [`nightOverrides/${key}`]: expected },
      );
      return versions[`nightOverrides/${key}`] ?? 0;
    },
    Boolean(connection?.busy) || !nightOptions.includes(night),
    demo?.timingSpans,
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
  const unprocessed = connection?.unprocessed ?? demo!.unprocessed;
  const latestNight = results.nights.find((n) => n.night === night);
  const nightlyPlayers = new Map(
    latestNight?.players.map((row) => [row.id, row]),
  );
  const visiblePlayers = players
    .filter((p) =>
      `${p.name} ${p.className}`.toLowerCase().includes(search.toLowerCase()),
    )
    .sort((a, b) => {
      const byName =
        a.name.localeCompare(b.name, "en", { sensitivity: "base" }) ||
        a.id.localeCompare(b.id);
      let compared = byName;
      if (overrideSort.key !== "name") {
        const left = nightlyPlayers.get(a.id)?.[overrideSort.key];
        const right = nightlyPlayers.get(b.id)?.[overrideSort.key];
        // Missing results stay last in either direction; recorded zero is a value.
        if (left == null || right == null)
          return left == null ? (right == null ? byName : 1) : -1;
        compared = left - right;
      }
      return (
        (overrideSort.direction === "ascending" ? compared : -compared) ||
        byName
      );
    });

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
    if (demo?.notification)
      notify(demo.notification.text, demo.notification.error);
  }, [demo?.notification]);
  useEffect(() => {
    const handle = (e: BeforeUnloadEvent) => {
      if (unsaved || connection?.busy) {
        e.preventDefault();
      }
    };
    window.addEventListener("beforeunload", handle);
    return () => window.removeEventListener("beforeunload", handle);
  }, [unsaved, connection?.busy]);

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
      connection?.busy ||
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
    try {
      await (connection ? connection.requestRun() : demo!.requestRun());
    } catch (error) {
      notify((error as Error).message, true);
    }
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
  const resultsView = useResultsView({
    results,
    players,
    navigation,
    selectionReady: !connection?.resultsPending && !connection?.resultsError,
    demo: Boolean(demo),
    notify,
  });
  const { setResultNight, visibleResultNight } = resultsView;
  const lastFailed = history[0]?.status === "failed";
  const runStatusMessage = active
    ? active.status === "queued" && !online
      ? "Queued · waiting for your local bot"
      : `${steps[active.stage]}…`
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
                  : "";
  const waitingForResults = Boolean(
    connection?.submitting || connection?.resultsPending,
  );
  const resultLoadFailed = Boolean(connection?.resultsError);
  const completedAt =
    !connection || connection.hasResults ? results.completedAt : undefined;
  return (
    <AppShell
      page={page}
      sidebar={
        <Sidebar
          connection={connection}
          canManage={canManage}
          page={page}
          go={go}
          reportCount={inputs.reports.length}
          runButton={runButton}
          active={active}
          online={online}
          authPending={authPending}
          accountAction={accountAction}
          guard={guard}
          demoControls={
            demo &&
            renderDemoControls(() =>
              guard(() => {
                demo!.reset();
                nightChosen.current = false;
                setNight(defaultNight);
                setSelected("Example");
                availabilityEditor.cancel();
                timingEditor.cancel();
                setResultNight("season");
                notify("Sample data reset.");
              }),
            )
          }
        />
      }
      notices={
        <ConnectionNotices
          connection={connection}
          canManage={canManage}
          retryWithFocus={retryWithFocus}
        />
      }
      overlays={
        <>
          {" "}
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
        </>
      }
    >
      {page !== "results" && page !== "overrides" && (
        <div
          className={`reports-status toolbar ${active || connection?.submitting || waitingForResults ? "is-running" : ""} ${(lastFailed || resultLoadFailed) && !active && !connection?.submitting ? "has-error" : ""}`}
        >
          <RunStatus
            message={runStatusMessage}
            completedAt={
              !connection || connection.hasResults
                ? results.completedAt
                : undefined
            }
            detail={
              connection?.submitting
                ? "Waiting for the server to confirm your request."
                : active
                  ? "Your last completed results stay visible while Pebble works."
                  : unsaved
                    ? "Save or discard your edits before running Pebble."
                    : unprocessed
                      ? "Saved inputs have changed. Run Pebble to update the results."
                      : undefined
            }
          />
          {connection?.officer && connection.queueError && (
            <button
              className="text-button"
              onClick={() => retryWithFocus(connection.retryQueue)}
            >
              Retry run queue
            </button>
          )}
        </div>
      )}
      {demo?.storageError && (
        <Notice tone="amber">
          Browser storage is unavailable. The demo works, but changes won’t
          survive a reload.
        </Notice>
      )}

      {page === "reports" && (
        <ReportsPage
          reports={inputs.reports}
          editor={reportEditor}
          sampleReportLink={demo?.sampleReportLink}
        />
      )}
      {page === "settings" && connection?.officer && (
        <SettingsPage editor={calculationEditor} />
      )}
      {page === "roster" && connection?.officer && (
        <RosterPage
          connection={connection}
          players={players}
          inputs={inputs}
          guard={guard}
          rosterEditor={rosterEditor}
          characterEditor={characterEditor}
          rosterDetail={rosterDetail}
          setRosterDetail={setRosterDetail}
        />
      )}
      {page === "overrides" && (
        <OverridesPage
          nightSelection={{
            value: night,
            options: nightOptions,
            onChange: chooseNight,
          }}
          status={{
            message: runStatusMessage,
            completedAt,
            retryQueue:
              connection?.officer && connection.queueError
                ? () => retryWithFocus(connection.retryQueue)
                : undefined,
          }}
          tab={overrideTab}
          onTabChange={(next) => guard(() => setOverrideTab(next))}
          availability={{
            players,
            visiblePlayers,
            selectedPlayer,
            selected,
            search,
            setSearch,
            choosePlayer,
            sort: overrideSort,
            onSort: changeOverrideSort,
            inputs,
            latestNight,
            editor: availabilityEditor,
            span,
          }}
          timing={{ detected: detectedTiming, editor: timingEditor }}
        />
      )}
      {page === "results" && (
        <ResultsPage
          results={results}
          players={players}
          view={resultsView}
          hasResults={!connection || connection.hasResults}
          canManage={canManage}
          officer={Boolean(connection?.officer)}
          demo={Boolean(demo)}
          sources={connection?.resultSources ?? []}
          diagnostics={connection?.resultDiagnostics ?? {}}
          runStatusMessage={runStatusMessage}
          completedAt={completedAt}
          onEditAvailability={(id) =>
            guard(() => {
              const n =
                visibleResultNight === "season"
                  ? (results.nights[0]?.night ?? defaultNight)
                  : visibleResultNight;
              nightChosen.current = true;
              setNight(n);
              setSelected(id);
              setOverrideTab("availability");
              setPage("overrides");
            })
          }
        />
      )}
      {canManage && (
        <RunHistory history={history} connected={Boolean(connection)} />
      )}
    </AppShell>
  );
}
