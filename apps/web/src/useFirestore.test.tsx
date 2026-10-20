// @vitest-environment jsdom
import {
  act,
  cleanup,
  fireEvent,
  render,
  renderHook,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { User } from "firebase/auth";
import { useFirestore } from "./useFirestore";
import Root from "./Root";

type Data = Record<string, unknown>;
type Snapshot = {
  exists: () => boolean;
  metadata: { hasPendingWrites: boolean; fromCache?: boolean };
  data: () => Data;
  docs: {
    id: string;
    data: () => Data;
    metadata?: { hasPendingWrites: boolean };
  }[];
};
type Listener = {
  path: string;
  next: (s: Snapshot) => void;
  error: (e: Error & { code?: string }) => void;
  stop: ReturnType<typeof vi.fn>;
};
const mock = vi.hoisted(() => ({
  auth: undefined as unknown as (user: User | null) => void,
  authError: undefined as unknown as () => void,
  listeners: [] as Listener[],
  getDoc: vi.fn(),
  getDocs: vi.fn(),
  setDoc: vi.fn(),
  transaction: vi.fn(),
}));
vi.mock("./firebase", () => ({
  auth: {},
  db: {},
  configured: true,
  seasonId: "test",
  seasonLabel: "Test",
  signIn: vi.fn(),
  logOut: vi.fn(),
}));
vi.mock("firebase/auth", () => ({
  onAuthStateChanged: vi.fn((_auth, next, error) => {
    mock.auth = next;
    mock.authError = error;
    return vi.fn();
  }),
}));
vi.mock("firebase/firestore", () => ({
  doc: (_db: unknown, ...parts: string[]) => ({ path: parts.join("/") }),
  collection: (base: { path?: string }, ...parts: string[]) => ({
    path: [base.path, ...parts].filter(Boolean).join("/"),
  }),
  query: (ref: { path: string }, ...constraints: unknown[]) => ({
    path: constraints.includes("active-status")
      ? `${ref.path}/active`
      : ref.path,
  }),
  where: () => "active-status",
  orderBy: vi.fn(),
  limit: vi.fn(),
  serverTimestamp: vi.fn(),
  onSnapshot: (ref: { path: string }, ...args: unknown[]) => {
    const [next, error] = args.slice(-2) as [
      Listener["next"],
      Listener["error"],
    ];
    const stop = vi.fn();
    mock.listeners.push({ path: ref.path, next, error, stop });
    return stop;
  },
  getDoc: mock.getDoc,
  getDocFromServer: mock.getDoc,
  getDocs: mock.getDocs,
  getDocsFromServer: mock.getDocs,
  setDoc: mock.setDoc,
  runTransaction: mock.transaction,
}));
const snap = (data: Data = {}, docs: Record<string, Data> = {}): Snapshot => ({
  exists: () => true,
  data: () => data,
  metadata: { hasPendingWrites: false, fromCache: false },
  docs: Object.entries(docs).map(([id, d]) => ({
    id,
    data: () => d,
    metadata: { hasPendingWrites: false },
  })),
});
const latest = (path: string) => {
  const found = mock.listeners.filter((l) => l.path === path).at(-1);
  if (!found) throw new Error(`Missing listener: ${path}`);
  return found;
};
const send = (path: string, data: Data = {}, docs: Record<string, Data> = {}) =>
  act(() => {
    latest(path).next(snap(data, docs));
  });
const login = (uid = "officer") =>
  act(() => mock.auth({ uid, displayName: uid } as User));
const grant = (uid = "officer", queueLoaded = true) => {
  send(`officers/${uid}`, { enabled: true });
  if (queueLoaded) send("seasons/test/runs/active");
};
const revoke = () => send("officers/officer", { enabled: false });
const inputNames = [
  "reports",
  "players",
  "characters",
  "nightOverrides",
  "availabilityOverrides",
];
function loadPrivate() {
  for (const name of inputNames) send(`seasons/test/${name}`);
  send("seasons/test/settings/calculation");
  send(
    "seasons/test/reports",
    {},
    { aB3dE6gH9jK2mN5p: { enabled: true, notes: "private note", version: 3 } },
  );
  send(
    "seasons/test/availabilityOverrides",
    {},
    {
      private: {
        nightId: "2026-10-15",
        playerId: "secret",
        pre: { mode: "automatic" },
        post: { mode: "available" },
        notes: "private availability note",
        version: 2,
      },
    },
  );
  send(
    "seasons/test/nightOverrides",
    {},
    {
      "2026-10-15": {
        start: "",
        breakStart: "",
        breakEnd: "",
        end: "",
        notes: "private timing note",
        version: 2,
      },
    },
  );
  send("seasons/test/players", {}, { secret: { name: "Private roster name" } });
  send(
    "seasons/test/runs",
    {},
    {
      failed: {
        status: "failed",
        error: "private failure",
        completedAt: { toDate: () => new Date("2026-10-15T23:00:00Z") },
      },
      queued: { status: "queued" },
    },
  );
  send("bot/status", { lastHeartbeatAt: { toMillis: () => Date.now() } });
}
async function publicResults() {
  await act(async () =>
    latest("publicSeasons/test").next(snap({ latestRunId: "ready" })),
  );
}
function deferred<T>() {
  let resolve!: (v: T) => void;
  let reject!: (e: Error) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}
beforeEach(() => {
  window.history.replaceState(null, "", "/");
  vi.clearAllMocks();
  mock.getDoc.mockReset();
  mock.getDocs.mockReset();
  mock.setDoc.mockReset();
  mock.transaction.mockReset();
  mock.transaction.mockImplementation(async (_db, callback) => {
    const writes: Promise<unknown>[] = [];
    const result = await callback({
      get: mock.getDoc,
      set: (ref: unknown, data: unknown) => {
        writes.push(Promise.resolve(mock.setDoc(ref, data)));
      },
    });
    await Promise.all(writes);
    return result;
  });
  vi.spyOn(window, "scrollTo").mockImplementation(() => {});
  HTMLDialogElement.prototype.close = vi.fn();
  HTMLDialogElement.prototype.showModal = vi.fn();
  mock.listeners = [];
  mock.getDoc.mockResolvedValue(
    snap({
      complete: true,
      reportCount: 1,
      completedAt: { toDate: () => new Date("2026-10-15T23:00:00Z") },
    }),
  );
  mock.getDocs.mockResolvedValue(snap({}, {}));
});
afterEach(cleanup);

it("maps recorded report check timestamps and follows the newest available summary", () => {
  const { result } = renderHook(useFirestore);
  login();
  grant();
  send(
    "seasons/test/reports",
    {},
    { AAAAAAAAAAAAAAAA: { enabled: true, version: 1 } },
  );
  expect(result.current.connection.saved.inputs.reports[0].checkedAt).toBe("");
  const summary = (checkedAt: string) => ({
    title: "Example raid",
    night: "2026-10-15",
    checkedAt: stamp(checkedAt),
  });
  send(
    "seasons/test/runs",
    {},
    {
      newer: {
        reportSummaries: { AAAAAAAAAAAAAAAA: summary("2026-10-17T02:05:00Z") },
      },
      older: {
        reportSummaries: { AAAAAAAAAAAAAAAA: summary("2026-10-16T02:05:00Z") },
      },
    },
  );
  expect(result.current.connection.saved.inputs.reports[0]).toMatchObject({
    fetched: true,
    title: "Example raid",
    checkedAt: "2026-10-17T02:05:00.000Z",
  });
  send(
    "seasons/test/runs",
    {},
    {
      legacy: {
        reportSummaries: {
          AAAAAAAAAAAAAAAA: { title: "Example raid", night: "2026-10-15" },
        },
      },
    },
  );
  expect(result.current.connection.saved.inputs.reports[0].checkedAt).toBe("");
  expect(mock.transaction).not.toHaveBeenCalled();
});

describe("officer access lifecycle", () => {
  it("restores heading focus after officer-access retry remounts the workspace", async () => {
    render(<Root />);
    login();
    grant();
    loadPrivate();
    await publicResults();
    act(() =>
      latest("officers/officer").error(new Error("Lookup unavailable")),
    );
    const retry = screen.getByRole("button", { name: "Retry officer access" });
    retry.focus();
    fireEvent.click(retry);
    grant();
    loadPrivate();
    await waitFor(() =>
      expect(document.activeElement).toBe(
        document.getElementById("page-title"),
      ),
    );
  });

  it("keeps an empty officer roster separate from historical public result players", async () => {
    mock.getDocs.mockImplementation(async (ref: { path: string }) =>
      snap(
        {},
        ref.path.endsWith("/players")
          ? {
              former: {
                name: "Former Player",
                className: "Mage",
                role: "Damage",
              },
            }
          : {},
      ),
    );
    const { result } = renderHook(useFirestore);
    login();
    grant();
    send("seasons/test/players");
    await publicResults();
    expect(result.current.connection.saved.results.players[0].name).toBe(
      "Former Player",
    );
    expect(result.current.connection.players).toEqual([]);
    revoke();
    expect(result.current.connection.players[0].name).toBe("Former Player");
  });

  it("clears all private state on revocation, stops listeners and retains public results", async () => {
    const { result } = renderHook(useFirestore);
    login();
    grant();
    loadPrivate();
    await publicResults();
    send("seasons/test/runs/ready", {
      inputVersions: { "reports/aB3dE6gH9jK2mN5p": 3 },
    });
    const old = result.current.connection;
    expect(old.saved.inputs.reports[0].notes).toBe("private note");
    expect(old.online).toBe(true);
    const privateListeners = mock.listeners.filter(
      (l) => l.path !== "publicSeasons/test" && !l.path.startsWith("officers/"),
    );
    revoke();
    const c = result.current.connection;
    expect(c.officer).toBe(false);
    expect(c.saved.inputs).toEqual({
      reports: [],
      adjustments: {},
      timings: {},
    });
    expect(c.saved.history).toEqual([]);
    expect(c.versions).toEqual({});
    expect(c.active).toBeNull();
    expect(c.online).toBe(false);
    expect(c.busy).toBe(false);
    expect(c.players).toEqual([]);
    expect(c.hasResults).toBe(true);
    expect(c.saved.results.signature).toBe("ready");
    expect(result.current.loading).toBe(false);
    privateListeners.forEach((l) => expect(l.stop).toHaveBeenCalled());
    // SDK callbacks already queued before unsubscribe must also be harmless.
    act(() => {
      privateListeners.forEach((l) => {
        l.next(snap({}, { leaked: { notes: "secret", version: 99 } }));
        l.error(new Error("late private error"));
      });
    });
    expect(result.current.connection.saved.inputs.reports).toEqual([]);
    expect(result.current.connection.error).toBe("");
    await expect(old.requestRun()).rejects.toThrow("Officer access changed");
    await expect(old.saveInputs(old.saved.inputs)).rejects.toThrow(
      "Officer access changed",
    );
    expect(mock.setDoc).not.toHaveBeenCalled();
    grant();
    loadPrivate();
    expect(result.current.connection.officer).toBe(true);
    expect(result.current.connection.saved.inputs.reports).toHaveLength(1);
    expect(result.current.connection.accessGeneration).toBeGreaterThan(
      old.accessGeneration,
    );
    // Even after re-granting access, the old closures/callbacks are invalid.
    await expect(old.requestRun()).rejects.toThrow("Officer access changed");
  });

  it.each([null, "other"])(
    "clears state on auth change to %s and ignores old membership callbacks",
    async (uid) => {
      const { result } = renderHook(useFirestore);
      login();
      grant();
      loadPrivate();
      await publicResults();
      const oldMembership = latest("officers/officer");
      act(() => mock.auth(uid ? ({ uid } as User) : null));
      act(() => oldMembership.next(snap({ enabled: true })));
      expect(result.current.connection.officer).toBe(false);
      expect(result.current.connection.saved.history).toEqual([]);
      expect(result.current.connection.saved.inputs.reports).toEqual([]);
      expect(result.current.connection.hasResults).toBe(true);
      if (uid) {
        grant(uid);
        loadPrivate();
        expect(result.current.connection.officer).toBe(true);
      }
    },
  );

  it.each([
    "officers/officer",
    "seasons/test/reports",
    "seasons/test/runs/ready",
  ])("fails closed on permission loss from %s", async (path) => {
    const { result } = renderHook(useFirestore);
    login();
    grant();
    loadPrivate();
    await publicResults();
    act(() =>
      latest(path).error(
        Object.assign(new Error("Access denied"), {
          code: "permission-denied",
        }),
      ),
    );
    expect(result.current.connection.officer).toBe(false);
    expect(result.current.connection.saved.inputs.reports).toEqual([]);
    expect(result.current.connection.error).toBe("");
    expect(result.current.connection.accessError).toContain("retry access");
    expect(result.current.loading).toBe(false);
  });

  it("reloads membership if Auth emits the same user object again", () => {
    const { result } = renderHook(useFirestore);
    const user = { uid: "officer" } as User;
    act(() => mock.auth(user));
    grant();
    loadPrivate();
    const old = latest("officers/officer");
    act(() => mock.auth(user));
    expect(old.stop).toHaveBeenCalled();
    expect(latest("officers/officer")).not.toBe(old);
    grant();
    loadPrivate();
    expect(result.current.connection.officer).toBe(true);
  });

  it("does not hang loading when the initial membership lookup fails", () => {
    const { result } = renderHook(useFirestore);
    login();
    send("publicSeasons/test");
    act(() =>
      latest("officers/officer").error(new Error("Membership unavailable")),
    );
    expect(result.current.loading).toBe(false);
    expect(result.current.connection.accessError).toContain("retry access");
    expect(result.current.connection.accessError).not.toContain(
      "Membership unavailable",
    );
    expect(result.current.connection.officer).toBe(false);
  });

  it("abandons a submission lookup on revocation and uses a new request ID after re-grant", async () => {
    const { result } = renderHook(useFirestore);
    login();
    grant();
    loadPrivate();
    const lookup = deferred<Snapshot>();
    mock.getDoc.mockReturnValueOnce(lookup.promise);
    let request!: Promise<void>;
    act(() => {
      request = result.current.connection.requestRun();
    });
    const firstRef = mock.getDoc.mock.calls[0][0];
    revoke();
    grant();
    loadPrivate();
    // The stale request must not unset busy on a newer request.
    const nextLookup = deferred<Snapshot>();
    mock.getDoc.mockReturnValueOnce(nextLookup.promise);
    let next!: Promise<void>;
    act(() => {
      next = result.current.connection.requestRun();
    });
    expect(mock.getDoc.mock.calls[1][0].path).not.toBe(firstRef.path);
    await act(async () => {
      lookup.resolve({ ...snap(), exists: () => false });
      await expect(request).rejects.toThrow("Officer access changed");
    });
    expect(result.current.connection.busy).toBe(true);
    expect(mock.setDoc).not.toHaveBeenCalled();
    await act(async () => {
      nextLookup.resolve({ ...snap(), exists: () => false });
      await next;
    });
    expect(mock.setDoc).toHaveBeenCalledTimes(1);
    expect(result.current.connection.busy).toBe(false);
  });

  it("does not apply a late submission failure to a new access session", async () => {
    const { result } = renderHook(useFirestore);
    login();
    grant();
    loadPrivate();
    const write = deferred<void>();
    mock.setDoc.mockReturnValueOnce(write.promise);
    mock.getDoc.mockResolvedValueOnce({ ...snap(), exists: () => false });
    let request!: Promise<void>;
    await act(async () => {
      request = result.current.connection.requestRun();
    });
    revoke();
    grant();
    loadPrivate();
    await act(async () => {
      write.reject(new Error("Old private failure"));
      await expect(request).rejects.toThrow("Old private failure");
    });
    expect(result.current.connection.error).toBe("");
  });

  it("returns saved versions and writes only the supplied availability override", async () => {
    const { result } = renderHook(useFirestore);
    login();
    grant();
    loadPrivate();
    const set = vi.fn();
    mock.transaction.mockImplementationOnce(async (_db, callback) =>
      callback({
        get: async (ref: { path: string }) => ({
          ...snap({ version: 2 }),
          ref,
        }),
        set,
      }),
    );
    let acknowledged!: Record<string, number>;
    await act(async () => {
      acknowledged = await result.current.connection.saveInputs(
        {
          reports: [],
          timings: {},
          adjustments: {
            "2026-10-15/secret": {
              pre: { mode: "available" },
              post: { mode: "automatic" },
              notes: "Only this override",
            },
          },
        },
        { "availabilityOverrides/2026-10-15_secret": 2 },
      );
    });
    expect(acknowledged["availabilityOverrides/2026-10-15_secret"]).toBe(3);
    expect(set).toHaveBeenCalledTimes(1);
    expect(set.mock.calls[0][0].path).toBe(
      "seasons/test/availabilityOverrides/2026-10-15_secret",
    );
    expect(set.mock.calls[0][1].version).toBe(3);
    expect(result.current.connection.busy).toBe(false);
    const noChanges = await result.current.connection.saveInputs({
      reports: [],
      timings: {},
      adjustments: {},
    });
    expect(noChanges).toEqual(result.current.connection.versions);
    expect(mock.transaction).toHaveBeenCalledTimes(1);
  });

  it("stops an in-flight save before transaction writes and ignores its stale failure", async () => {
    const { result } = renderHook(useFirestore);
    login();
    grant();
    loadPrivate();
    const read = deferred<Snapshot>();
    const set = vi.fn();
    mock.transaction.mockImplementationOnce(async (_db, callback) =>
      callback({ get: () => read.promise, set }),
    );
    let save!: Promise<unknown>;
    act(() => {
      save = result.current.connection.saveInputs({
        ...result.current.connection.saved.inputs,
        reports: [],
        timings: {
          "2026-10-15": {
            start: "",
            breakStart: "",
            breakEnd: "",
            end: "",
            notes: "private draft",
          },
        },
      });
    });
    revoke();
    await act(async () => {
      read.resolve(snap());
      await expect(save).rejects.toThrow("Officer access changed");
    });
    expect(set).not.toHaveBeenCalled();
    expect(result.current.connection.error).toBe("");
    expect(result.current.connection.busy).toBe(false);
  });

  it("discards actual editing drafts across revocation and re-grant", async () => {
    render(<Root />);
    login();
    grant();
    loadPrivate();
    await publicResults();
    const textbox = screen.getByRole("textbox", {
      name: "Warcraft Logs reports",
    });
    fireEvent.change(textbox, { target: { value: "private unsaved draft" } });
    expect((textbox as HTMLTextAreaElement).value).toBe(
      "private unsaved draft",
    );
    revoke();
    expect(
      screen.queryByRole("textbox", { name: "Warcraft Logs reports" }),
    ).toBeNull();
    expect(
      screen.getByText(/This account does not have officer access/),
    ).toBeTruthy();
    grant();
    loadPrivate();
    await waitFor(() =>
      expect(
        (
          screen.getByRole("textbox", {
            name: "Warcraft Logs reports",
          }) as HTMLTextAreaElement
        ).value,
      ).toBe(""),
    );
    expect(screen.queryByText("private unsaved draft")).toBeNull();
    const freshTextbox = screen.getByRole("textbox", {
      name: "Warcraft Logs reports",
    });
    fireEvent.change(freshTextbox, {
      target: { value: "another private draft" },
    });
    // React may batch several access transitions into one render. The generation
    // key must still discard local drafts if the final officer flag is true.
    act(() => {
      revoke();
      grant();
    });
    loadPrivate();
    expect(
      (
        screen.getByRole("textbox", {
          name: "Warcraft Logs reports",
        }) as HTMLTextAreaElement
      ).value,
    ).toBe("");
  });
});

const missing = (): Snapshot => ({ ...snap(), exists: () => false });
const stamp = (iso = "2026-10-16T23:00:00Z") => ({
  toDate: () => new Date(iso),
});
const completed = (): Snapshot =>
  snap({ complete: true, completedAt: stamp(), reportCount: 2 });

describe("queue recovery", () => {
  it("waits for the server queue and offers a working retry after listener failure", async () => {
    render(<Root />);
    login();
    grant("officer", false);
    loadPrivate();
    await publicResults();
    const button = () =>
      screen.getByRole("button", { name: "Run Pebble" }) as HTMLButtonElement;
    expect(button().disabled).toBe(true);
    expect(
      screen.getByRole("status", { name: "Run status" }).textContent,
    ).toContain("Checking run queue");
    const cached = snap();
    cached.metadata.fromCache = true;
    act(() => latest("seasons/test/runs/active").next(cached));
    expect(button().disabled).toBe(true);
    const old = latest("seasons/test/runs/active");
    act(() => old.error(new Error("Queue unavailable")));
    expect(button().disabled).toBe(true);
    expect(
      screen.getByRole("status", { name: "Run status" }).textContent,
    ).toContain("Run queue is unavailable");
    fireEvent.click(screen.getByRole("button", { name: "Retry run queue" }));
    expect(old.stop).toHaveBeenCalled();
    act(() => old.next(snap()));
    expect(button().disabled).toBe(true);
    send("seasons/test/runs/active");
    expect(button().disabled).toBe(false);
    expect(
      screen.queryByRole("button", { name: "Retry run queue" }),
    ).toBeNull();
    expect(mock.transaction).not.toHaveBeenCalled();
  });

  it("recovers a run outside history, prefers running work, and pins terminal details", async () => {
    const { result } = renderHook(useFirestore);
    login();
    grant();
    const queued = {
      status: "queued",
      requestedAt: stamp("2026-10-15T20:00:00Z"),
    };
    const running = {
      status: "running",
      stage: "Fetching reports",
      requestedAt: stamp(),
    };
    send("seasons/test/runs", {}, { newer: { status: "failed" } });
    send("seasons/test/runs/active", {}, { old: queued, running });
    expect(result.current.connection.active).toMatchObject({
      id: "running",
      status: "running",
      stage: 2,
    });
    // Direct tracking stays conservative until both query and document agree.
    send("seasons/test/runs/active", {}, { old: queued });
    send("seasons/test/runs/running", {
      ...running,
      status: "failed",
      error: "Finished outside history",
    });
    expect(result.current.connection.active?.id).toBe("old");
    send("seasons/test/runs/active");
    send("seasons/test/runs/old", {
      ...queued,
      status: "failed",
      error: "Recovered run stopped",
    });
    expect(result.current.connection.active).toBeNull();
    expect(
      result.current.connection.saved.history.some(
        (r) => r.message === "Recovered run stopped",
      ),
    ).toBe(true);
    expect(mock.transaction).not.toHaveBeenCalled();
  });

  it("keeps observed run data when queue confirmation arrives before submission acknowledgement", async () => {
    const { result } = renderHook(useFirestore);
    login();
    grant();
    const write = deferred<void>();
    mock.getDoc.mockResolvedValueOnce(missing());
    mock.setDoc.mockReturnValueOnce(write.promise);
    let request!: Promise<void>;
    await act(async () => {
      request = result.current.connection.requestRun();
    });
    const path = mock.getDoc.mock.calls[0][0].path;
    const id = path.split("/").at(-1)!;
    send(
      "seasons/test/runs/active",
      {},
      { [id]: { status: "queued", requestedAt: stamp() } },
    );
    send(path, { status: "queued", requestedAt: stamp() });
    await act(async () => {
      write.resolve();
      await request;
    });
    expect(result.current.connection.submitting).toBe(false);
    expect(result.current.connection.active?.id).toBe(id);
    expect(mock.setDoc).toHaveBeenCalledTimes(1);
  });

  it("rejects submission before queue confirmation and ignores queue callbacks after access loss", async () => {
    const { result } = renderHook(useFirestore);
    login();
    grant("officer", false);
    await expect(result.current.connection.requestRun()).rejects.toThrow(
      "Wait for the run queue",
    );
    const old = latest("seasons/test/runs/active");
    revoke();
    act(() => {
      old.next(snap({}, { hidden: { status: "queued" } }));
      old.error(new Error("Old queue failure"));
    });
    expect(result.current.connection.active).toBeNull();
    expect(result.current.connection.queueReady).toBe(false);
    expect(result.current.connection.queueError).toBe("");
    expect(old.stop).toHaveBeenCalled();
    expect(mock.transaction).not.toHaveBeenCalled();
  });
});

describe("run submission", () => {
  it("disables the actual Run button while submitting and until the run is observed", async () => {
    render(<Root />);
    login();
    grant();
    loadPrivate();
    send(
      "seasons/test/runs",
      {},
      {
        prior: {
          status: "failed",
          requestedAt: stamp(),
          completedAt: stamp(),
          error: "Prior failure",
        },
      },
    );
    await publicResults();
    const lookup = deferred<Snapshot>();
    mock.getDoc.mockReturnValueOnce(lookup.promise);
    fireEvent.click(screen.getByRole("button", { name: "Run Pebble" }));
    expect(
      (
        screen.getByRole("button", {
          name: "Submitting run…",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    expect(
      screen.getByRole("status", { name: "Run status" }).textContent,
    ).toContain("Waiting for the server to confirm");
    expect(
      screen
        .getByRole("status", { name: "Run status" })
        .closest(".run-strip")!
        .classList.contains("is-running"),
    ).toBe(true);
    expect(
      screen
        .getByRole("status", { name: "Run status" })
        .closest(".run-strip")!
        .classList.contains("has-error"),
    ).toBe(false);
    await act(async () => {
      lookup.resolve(missing());
    });
    expect(
      (
        screen.getByRole("button", {
          name: "Submitting run…",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    send(mock.getDoc.mock.calls.at(-1)![0].path, { status: "queued" });
    expect(
      (screen.getByRole("button", { name: "Run queued" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
  });

  it("coalesces rapid calls and blocks another request until the confirmed run finishes", async () => {
    const { result } = renderHook(useFirestore);
    login();
    grant();
    loadPrivate();
    send("seasons/test/runs");
    const lookup = deferred<Snapshot>();
    mock.getDoc.mockReturnValueOnce(lookup.promise);
    let first!: Promise<void>, second!: Promise<void>;
    act(() => {
      first = result.current.connection.requestRun();
      second = result.current.connection.requestRun();
    });
    expect(mock.getDoc).toHaveBeenCalledTimes(1);
    expect(result.current.connection.submitting).toBe(true);
    await act(async () => {
      lookup.resolve(missing());
      await Promise.all([first, second]);
    });
    expect(mock.setDoc).toHaveBeenCalledTimes(1);
    const ref = mock.getDoc.mock.calls[0][0];
    await act(async () => {
      await result.current.connection.requestRun();
    });
    expect(mock.getDoc).toHaveBeenCalledTimes(1);
    expect(result.current.connection.submitting).toBe(true);
    send(ref.path, { status: "queued", requestedAt: stamp() });
    expect(result.current.connection.submitting).toBe(false);
    expect(result.current.connection.active?.id).toBe(
      ref.path.split("/").at(-1),
    );
    // The bounded recent query can omit this request without hiding it.
    send(
      "seasons/test/runs",
      {},
      { other: { status: "succeeded", requestedAt: stamp() } },
    );
    expect(result.current.connection.active?.status).toBe("queued");
    send(ref.path, {
      status: "failed",
      requestedAt: stamp(),
      completedAt: stamp(),
      error: "Source unavailable",
    });
    expect(result.current.connection.active).toBeNull();
    mock.getDoc.mockResolvedValueOnce(missing());
    await act(async () => {
      await result.current.connection.requestRun();
    });
    expect(mock.getDoc.mock.calls[1][0].path).not.toBe(ref.path);
    expect(mock.setDoc).toHaveBeenCalledTimes(2);
  });

  it("retries an uncertain write with the same ID and recognizes the already-created request", async () => {
    const { result } = renderHook(useFirestore);
    login();
    grant();
    loadPrivate();
    mock.getDoc.mockResolvedValueOnce(missing());
    mock.setDoc.mockRejectedValueOnce(new Error("Response lost"));
    await act(async () => {
      await expect(result.current.connection.requestRun()).rejects.toThrow(
        "Response lost",
      );
    });
    expect(result.current.connection.busy).toBe(false);
    mock.getDoc.mockResolvedValueOnce(
      snap({ status: "queued", requestedBy: "officer" }),
    );
    await act(async () => {
      await result.current.connection.requestRun();
    });
    expect(mock.getDoc.mock.calls[0][0].path).toBe(
      mock.getDoc.mock.calls[1][0].path,
    );
    expect(mock.setDoc).toHaveBeenCalledTimes(1);
    expect(result.current.connection.error).toBe("");
    send(mock.getDoc.mock.calls[1][0].path, { status: "queued" });
    expect(result.current.connection.active?.status).toBe("queued");
  });

  it("does not call a locally pending write queued before server confirmation", async () => {
    const { result } = renderHook(useFirestore);
    login();
    grant();
    loadPrivate();
    send("seasons/test/runs");
    const local = snap({}, { pending: { status: "queued" } });
    local.docs[0].metadata = { hasPendingWrites: true };
    act(() => latest("seasons/test/runs").next(local));
    expect(result.current.connection.active).toBeNull();
    mock.getDoc.mockResolvedValueOnce(missing());
    await act(async () => {
      await result.current.connection.requestRun();
    });
    const path = mock.getDoc.mock.calls[0][0].path;
    act(() =>
      latest(path).next({
        ...snap({ status: "queued" }),
        metadata: { hasPendingWrites: true },
      }),
    );
    expect(result.current.connection.active).toBeNull();
    expect(result.current.connection.submitting).toBe(true);
    send(path, { status: "queued" });
    expect(result.current.connection.active?.status).toBe("queued");
  });

  it("can reconnect to a failed run listener without creating a second request", async () => {
    const { result } = renderHook(useFirestore);
    login();
    grant();
    loadPrivate();
    send("seasons/test/runs");
    mock.getDoc.mockResolvedValueOnce(missing());
    await act(async () => {
      await result.current.connection.requestRun();
    });
    const path = mock.getDoc.mock.calls[0][0].path;
    const old = latest(path);
    act(() => old.error(new Error("Run listener unavailable")));
    expect(result.current.connection.submitting).toBe(false);
    mock.getDoc.mockResolvedValueOnce(snap({ status: "queued" }));
    await act(async () => {
      await result.current.connection.requestRun();
    });
    expect(mock.getDoc.mock.calls[1][0].path).toBe(path);
    expect(mock.setDoc).toHaveBeenCalledTimes(1);
    expect(old.stop).toHaveBeenCalled();
    send(path, { status: "running", stage: "Fetching reports" });
    expect(result.current.connection.active?.stage).toBe(2);
  });

  it("ignores tracked-run callbacks after access is revoked", async () => {
    const { result } = renderHook(useFirestore);
    login();
    grant();
    loadPrivate();
    mock.getDoc.mockResolvedValueOnce(missing());
    await act(async () => {
      await result.current.connection.requestRun();
    });
    const old = latest(mock.getDoc.mock.calls[0][0].path);
    revoke();
    grant();
    loadPrivate();
    send("seasons/test/runs");
    act(() => {
      old.next(snap({ status: "running" }));
      old.error(new Error("Old error"));
    });
    expect(result.current.connection.active).toBeNull();
    expect(result.current.connection.error).toBe("");
    expect(old.stop).toHaveBeenCalled();
  });
});

describe("public result loading", () => {
  it("keeps source reports tied to the displayed run and clears them on replacement/access loss", async () => {
    const { result } = renderHook(useFirestore);
    login();
    grant();
    await publicResults();
    const old = latest("seasons/test/runs/ready");
    send("seasons/test/runs/ready", {
      reportSummaries: {
        AAAAAAAAAAAAAAAA: { title: "Original source" },
        BBBBBBBBBBBBBBBB: { title: "  " },
      },
    });
    expect(result.current.connection.resultSources).toEqual([
      { code: "AAAAAAAAAAAAAAAA", title: "Original source" },
      { code: "BBBBBBBBBBBBBBBB", title: "BBBBBBBBBBBBBBBB" },
    ]);
    send(
      "seasons/test/runs",
      {},
      {
        newer: {
          reportSummaries: {
            CCCCCCCCCCCCCCCC: { title: "Unpublished source" },
          },
        },
      },
    );
    expect(result.current.connection.resultSources[0].title).toBe(
      "Original source",
    );
    const players = deferred<Snapshot>();
    mock.getDoc.mockResolvedValueOnce(completed());
    mock.getDocs.mockReturnValueOnce(players.promise);
    send("publicSeasons/test", { latestRunId: "new" });
    await act(async () => {});
    expect(result.current.connection.resultSources[0].title).toBe(
      "Original source",
    );
    await act(async () => players.resolve(snap()));
    expect(result.current.connection.resultSources).toEqual([]);
    act(() =>
      old.next(
        snap({
          reportSummaries: { AAAAAAAAAAAAAAAA: { title: "Stale source" } },
        }),
      ),
    );
    expect(result.current.connection.resultSources).toEqual([]);
    send("seasons/test/runs/new", {
      reportSummaries: { CCCCCCCCCCCCCCCC: { title: "New source" } },
    });
    expect(result.current.connection.resultSources).toEqual([
      { code: "CCCCCCCCCCCCCCCC", title: "New source" },
    ]);
    revoke();
    expect(result.current.connection.resultSources).toEqual([]);
    expect(result.current.connection.saved.results.signature).toBe("new");
    grant();
    send("seasons/test/runs/new");
    expect(result.current.connection.resultSources).toEqual([]);
  });

  it("keeps warnings with the completed view through failed reads, retry and clearing", async () => {
    const { result } = renderHook(useFirestore);
    mock.getDoc.mockResolvedValueOnce(
      snap({ ...completed().data(), warnings: ["Baseline warning"] }),
    );
    await publicResults();
    expect(result.current.connection.saved.results.warnings).toEqual([
      "Baseline warning",
    ]);
    const players = deferred<Snapshot>();
    mock.getDoc.mockResolvedValueOnce(
      snap({ ...completed().data(), warnings: ["New warning"] }),
    );
    mock.getDocs.mockReturnValueOnce(players.promise);
    send("publicSeasons/test", { latestRunId: "new" });
    await act(async () => {});
    expect(result.current.connection.saved.results.warnings).toEqual([
      "Baseline warning",
    ]);
    await act(async () => players.reject(new Error("Read unavailable")));
    expect(result.current.connection.resultsError).not.toBe("");
    expect(result.current.connection.saved.results.warnings).toEqual([
      "Baseline warning",
    ]);
    act(() => result.current.connection.retryResults());
    mock.getDoc.mockResolvedValueOnce(
      snap({ ...completed().data(), warnings: ["New warning"] }),
    );
    await act(async () =>
      latest("publicSeasons/test").next(snap({ latestRunId: "new" })),
    );
    expect(result.current.connection.saved.results.signature).toBe("new");
    expect(result.current.connection.saved.results.warnings).toEqual([
      "New warning",
    ]);
    mock.getDoc.mockResolvedValueOnce(completed());
    await act(async () =>
      latest("publicSeasons/test").next(
        snap({ latestRunId: "without-warnings" }),
      ),
    );
    expect(result.current.connection.saved.results.warnings).toEqual([]);
    send("publicSeasons/test");
    expect(result.current.connection.saved.results.warnings).toEqual([]);
  });

  it("waits for weekly reads and retains all prior results after a weekly failure", async () => {
    const { result } = renderHook(useFirestore);
    mock.getDocs.mockImplementation((ref: { path: string }) =>
      Promise.resolve(
        ref.path.endsWith("/weeks")
          ? snap(
              {},
              {
                "2026-10-06": {
                  players: [
                    {
                      main: "Previous Player",
                      played_min: 60,
                      bench_min: 10,
                      bench_pre_min: 10,
                      bench_post_min: 0,
                    },
                  ],
                },
              },
            )
          : snap(),
      ),
    );
    await publicResults();
    const weeks = deferred<Snapshot>();
    mock.getDocs.mockImplementation((ref: { path: string }) =>
      ref.path.endsWith("/weeks") ? weeks.promise : Promise.resolve(snap()),
    );
    send("publicSeasons/test", { latestRunId: "new" });
    await act(async () => {});
    expect(result.current.connection.saved.results.signature).toBe("ready");
    expect(result.current.connection.resultsPending).toBe(true);
    await act(async () => weeks.reject(new Error("Weekly read failed")));
    expect(result.current.connection.saved.results.signature).toBe("ready");
    expect(result.current.connection.resultsError).toMatch(/Couldn’t load/);
    expect(
      result.current.connection.saved.results.weeks?.[0].players[0].name,
    ).toBe("Previous Player");
    mock.getDocs.mockImplementation((ref: { path: string }) =>
      Promise.resolve(
        ref.path.endsWith("/weeks")
          ? snap(
              {},
              {
                "2026-10-13": {
                  players: [
                    {
                      main: "Weekly Player",
                      played_min: 123,
                      bench_min: 47,
                      bench_pre_min: 20,
                      bench_post_min: 27,
                    },
                  ],
                },
              },
            )
          : snap(),
      ),
    );
    act(() => result.current.connection.retryResults());
    await act(async () =>
      latest("publicSeasons/test").next(snap({ latestRunId: "new" })),
    );
    expect(result.current.connection.saved.results.signature).toBe("new");
    expect(
      result.current.connection.saved.results.weeks?.[0].players[0],
    ).toMatchObject({ name: "Weekly Player", played: 123, bench: 47 });
    expect(result.current.connection.resultsError).toBe("");
  });

  it("waits for every child collection before replacing the displayed set", async () => {
    const { result } = renderHook(useFirestore);
    await publicResults();
    const players = deferred<Snapshot>();
    mock.getDoc.mockResolvedValueOnce(completed());
    mock.getDocs.mockReturnValueOnce(players.promise);
    send("publicSeasons/test", { latestRunId: "new" });
    await act(async () => {});
    expect(result.current.connection.saved.results.signature).toBe("ready");
    expect(result.current.connection.resultsPending).toBe(true);
    await act(async () => {
      players.resolve(snap({}, { p1: { id: "p1", played: 10, bench: 5 } }));
    });
    expect(result.current.connection.saved.results.signature).toBe("new");
    expect(result.current.connection.saved.results.players[0].played).toBe(10);
    expect(result.current.connection.resultsPending).toBe(false);
  });

  it("keeps input freshness tied to the displayed result snapshot", async () => {
    const { result } = renderHook(useFirestore);
    login();
    grant();
    loadPrivate();
    await publicResults();
    const captured = result.current.connection.versions;
    send("seasons/test/runs/ready", { inputVersions: captured });
    expect(result.current.connection.unprocessed).toBe(false);
    send(
      "seasons/test/runs",
      {},
      { new: { status: "succeeded", completedAt: stamp() } },
    );
    expect(result.current.connection.resultsPending).toBe(true);
    // A newer saved edit must not be claimed by the completed run.
    send(
      "seasons/test/reports",
      {},
      { aB3dE6gH9jK2mN5p: { enabled: true, version: 4 } },
    );
    mock.getDoc.mockResolvedValueOnce(completed());
    await act(async () => {
      latest("publicSeasons/test").next(snap({ latestRunId: "new" }));
    });
    send("seasons/test/runs/new", { inputVersions: captured });
    expect(result.current.connection.resultsPending).toBe(false);
    expect(result.current.connection.unprocessed).toBe(true);
    send("seasons/test/runs/new", {
      inputVersions: result.current.connection.versions,
    });
    expect(result.current.connection.unprocessed).toBe(false);
  });

  it("waits for the displayed run's captured versions and ignores abandoned metadata", async () => {
    const { result } = renderHook(useFirestore);
    login();
    grant();
    loadPrivate();
    await publicResults();
    const captured = result.current.connection.versions;
    send("seasons/test/runs/ready", { inputVersions: captured });
    expect(result.current.connection.unprocessed).toBe(false);
    const old = latest("seasons/test/runs/ready");
    await act(async () =>
      latest("publicSeasons/test").next(snap({ latestRunId: "new" })),
    );
    expect(result.current.connection.saved.results.signature).toBe("new");
    expect(result.current.connection.unprocessed).toBe(true);
    act(() => old.next(snap({ inputVersions: captured })));
    expect(result.current.connection.unprocessed).toBe(true);
    send("seasons/test/runs/new", { inputVersions: captured });
    expect(result.current.connection.unprocessed).toBe(false);
  });

  it("pins a complete view to the newest pointer when reads finish out of order", async () => {
    const { result } = renderHook(useFirestore);
    await publicResults();
    const older = deferred<Snapshot>(),
      newer = deferred<Snapshot>();
    mock.getDoc
      .mockReturnValueOnce(older.promise)
      .mockReturnValueOnce(newer.promise);
    send("publicSeasons/test", { latestRunId: "older" });
    send("publicSeasons/test", { latestRunId: "newer" });
    expect(result.current.connection.saved.results.signature).toBe("ready");
    expect(result.current.connection.resultsPending).toBe(true);
    await act(async () => {
      newer.resolve(completed());
    });
    expect(result.current.connection.saved.results.signature).toBe("newer");
    await act(async () => {
      older.resolve(completed());
    });
    expect(result.current.connection.saved.results.signature).toBe("newer");
    expect(result.current.connection.resultsPending).toBe(false);
  });

  it("retains the old complete set on child-read failure and retries without a page reload", async () => {
    const { result } = renderHook(useFirestore);
    await publicResults();
    mock.getDocs.mockRejectedValueOnce(new Error("Private SDK payload"));
    await act(async () => {
      latest("publicSeasons/test").next(snap({ latestRunId: "new" }));
    });
    expect(result.current.connection.saved.results.signature).toBe("ready");
    expect(result.current.connection.resultsError).toMatch(/Couldn’t load/);
    expect(result.current.connection.resultsError).not.toContain(
      "Private SDK payload",
    );
    const old = latest("publicSeasons/test");
    act(() => result.current.connection.retryResults());
    expect(old.stop).toHaveBeenCalled();
    expect(result.current.connection.resultsPending).toBe(true);
    await act(async () => {
      latest("publicSeasons/test").next(snap({ latestRunId: "new" }));
    });
    expect(result.current.connection.saved.results.signature).toBe("new");
    expect(result.current.connection.resultsError).toBe("");
    // A queued callback from the abandoned subscription cannot reset the view.
    act(() => {
      old.next(snap());
    });
    expect(result.current.connection.saved.results.signature).toBe("new");
  });

  it("never replaces a completed set with staging output", async () => {
    const { result } = renderHook(useFirestore);
    await publicResults();
    mock.getDoc.mockResolvedValueOnce(snap({ complete: false }));
    await act(async () => {
      latest("publicSeasons/test").next(snap({ latestRunId: "staging" }));
    });
    expect(result.current.connection.saved.results.signature).toBe("ready");
    expect(result.current.connection.resultsError).toMatch(/Couldn’t load/);
  });

  it("invalidates an in-flight read on subscription failure", async () => {
    const { result } = renderHook(useFirestore);
    await publicResults();
    const pending = deferred<Snapshot>();
    mock.getDoc.mockReturnValueOnce(pending.promise);
    send("publicSeasons/test", { latestRunId: "late" });
    act(() =>
      latest("publicSeasons/test").error(new Error("Connection failed")),
    );
    await act(async () => {
      pending.resolve(completed());
    });
    expect(result.current.connection.saved.results.signature).toBe("ready");
    expect(result.current.connection.resultsError).toMatch(/Couldn’t connect/);
  });

  it("invalidates in-flight reads when the pointer is removed or the hook unmounts", async () => {
    const { result, unmount } = renderHook(useFirestore);
    const pending = deferred<Snapshot>();
    mock.getDoc.mockReturnValueOnce(pending.promise);
    send("publicSeasons/test", { latestRunId: "late" });
    send("publicSeasons/test");
    await act(async () => {
      pending.resolve(completed());
    });
    expect(result.current.connection.hasResults).toBe(false);
    const second = deferred<Snapshot>();
    mock.getDoc.mockReturnValueOnce(second.promise);
    send("publicSeasons/test", { latestRunId: "after-unmount" });
    const old = latest("publicSeasons/test");
    unmount();
    const calls = mock.getDoc.mock.calls.length;
    await act(async () => {
      second.resolve(completed());
      old.next(snap({ latestRunId: "discarded" }));
    });
    expect(mock.getDoc).toHaveBeenCalledTimes(calls);
    expect(old.stop).toHaveBeenCalled();
    expect(result.current.connection.hasResults).toBe(false);
  });

  it("can finish a public read after an account change without restoring private state", async () => {
    const { result } = renderHook(useFirestore);
    login();
    grant();
    loadPrivate();
    const pending = deferred<Snapshot>();
    mock.getDoc.mockReturnValueOnce(pending.promise);
    send("publicSeasons/test", { latestRunId: "public" });
    act(() => mock.auth(null));
    await act(async () => {
      pending.resolve(completed());
    });
    expect(result.current.connection.saved.results.signature).toBe("public");
    expect(result.current.connection.saved.inputs.reports).toEqual([]);
    expect(result.current.connection.officer).toBe(false);
  });

  it("keeps the completion UI pending until the matching results load, retaining unprocessed edits", async () => {
    render(<Root />);
    login();
    grant();
    loadPrivate();
    await publicResults();
    send(
      "seasons/test/runs",
      {},
      {
        new: {
          status: "succeeded",
          requestedAt: stamp(),
          completedAt: stamp(),
        },
      },
    );
    expect(
      screen.getByRole("status", { name: "Run status" }).textContent,
    ).toContain("Loading latest results");
    expect(screen.queryByText("Results updated")).toBeNull();
    const pending = deferred<Snapshot>();
    mock.getDoc.mockReturnValueOnce(pending.promise);
    send("publicSeasons/test", { latestRunId: "new" });
    expect(
      screen.getByRole("status", { name: "Run status" }).textContent,
    ).toContain("Loading latest results");
    await act(async () => {
      pending.resolve(completed());
    });
    // Inputs saved after the captured snapshot still require another run.
    send("seasons/test/runs/new", {
      inputVersions: { "reports/aB3dE6gH9jK2mN5p": 2 },
    });
    expect(
      screen.getByRole("status", { name: "Run status" }).textContent,
    ).toContain("Changes ready for the next run");
    expect(screen.queryByText("Loading latest results…")).toBeNull();
  });

  it("resets a missing result period and keeps rows consistent when result nights change", async () => {
    let currentNight = "2026-10-14";
    let currentName = "Baseline Player";
    mock.getDocs.mockImplementation(async (ref: { path: string }) => {
      if (ref.path.endsWith("/weeks")) return snap();
      const row = {
        id: "p1",
        name: currentName,
        className: "Mage",
        played: 90,
        bench: 30,
        preBench: 10,
        postBench: 20,
        overridden: false,
        nights: 1,
      };
      return ref.path.endsWith("/players")
        ? snap({}, { p1: row })
        : snap(
            {},
            {
              night: {
                night: currentNight,
                duration: 120,
                reports: 1,
                players: [row],
              },
            },
          );
    });
    render(<Root />);
    act(() => mock.auth(null));
    await publicResults();
    fireEvent.change(screen.getByRole("combobox", { name: "Result period" }), {
      target: { value: currentNight },
    });
    expect(screen.getByText("Baseline Player")).toBeTruthy();
    currentNight = "2026-10-15";
    currentName = "New Player";
    await act(async () =>
      latest("publicSeasons/test").next(snap({ latestRunId: "new" })),
    );
    expect(
      (
        screen.getByRole("combobox", {
          name: "Result period",
        }) as HTMLSelectElement
      ).value,
    ).toBe("season");
    expect(screen.getByText("New Player")).toBeTruthy();
    expect(screen.queryByText("Baseline Player")).toBeNull();
    // A later result containing the former night must not silently reselect it.
    currentNight = "2026-10-14";
    currentName = "Later Player";
    await act(async () =>
      latest("publicSeasons/test").next(snap({ latestRunId: "later" })),
    );
    expect(
      (
        screen.getByRole("combobox", {
          name: "Result period",
        }) as HTMLSelectElement
      ).value,
    ).toBe("season");
    expect(screen.getByText("Later Player")).toBeTruthy();
  });

  it("shows a usable public error and retry control even before any results load", async () => {
    render(<Root />);
    act(() => mock.auth(null));
    mock.getDoc.mockRejectedValueOnce(new Error("Service unavailable"));
    await act(async () => {
      latest("publicSeasons/test").next(snap({ latestRunId: "ready" }));
    });
    expect(screen.getByRole("alert").textContent).toMatch(/Couldn’t load/);
    fireEvent.click(
      screen.getByRole("button", { name: "Retry loading results" }),
    );
    await publicResults();
    expect(screen.queryByRole("alert")).toBeNull();
    expect(screen.getByText(/Everyone’s time counts/)).toBeTruthy();
  });
});

it("fails closed on an input listener error and retries with a fresh access generation", () => {
  const { result } = renderHook(useFirestore);
  login();
  grant();
  send("publicSeasons/test");
  const oldMembership = latest("officers/officer");
  const oldInputs = latest("seasons/test/players");
  act(() => oldInputs.error(new Error("PRIVATE SDK PAYLOAD")));
  expect(result.current.loading).toBe(false);
  expect(result.current.connection.officer).toBe(false);
  expect(result.current.connection.accessError).toContain("retry access");
  expect(result.current.connection.accessError).not.toContain("PRIVATE");
  act(() => result.current.connection.retryAccess());
  expect(oldMembership.stop).toHaveBeenCalled();
  act(() => {
    oldMembership.next(snap({ enabled: true }));
    oldInputs.next(snap({}, { stale: { name: "PRIVATE stale input" } }));
  });
  expect(result.current.connection.officer).toBe(false);
  expect(result.current.connection.players).toEqual([]);
  grant();
  loadPrivate();
  expect(result.current.connection.officer).toBe(true);
  expect(result.current.connection.accessError).toBe("");
  expect(result.current.loading).toBe(false);
});
it("requires server-confirmed membership rather than an old cached grant", () => {
  const { result } = renderHook(useFirestore);
  login();
  act(() =>
    latest("officers/officer").next({
      ...snap({ enabled: true }),
      metadata: { hasPendingWrites: false, fromCache: true },
    }),
  );
  expect(result.current.connection.officer).toBe(false);
  expect(mock.listeners.some((l) => l.path === "seasons/test/players")).toBe(
    false,
  );
  grant();
  expect(result.current.connection.officer).toBe(true);
});
it.each(["auth", "membership", "inputs"])(
  "bounds initial %s waiting and keeps public access recoverable",
  (phase) => {
    vi.useFakeTimers();
    try {
      const { result, unmount } = renderHook(useFirestore);
      send("publicSeasons/test");
      if (phase !== "auth") login();
      if (phase === "inputs") grant();
      act(() => vi.advanceTimersByTime(15000));
      expect(result.current.loading).toBe(false);
      expect(result.current.connection.officer).toBe(false);
      expect(result.current.connection.accessError).toContain("retry access");
      expect(result.current.connection.players).toEqual([]);
      unmount();
    } finally {
      vi.useRealTimers();
    }
  },
);
it("recovers from Auth observer failure and ignores callbacks from its retired subscription", () => {
  const { result } = renderHook(useFirestore);
  send("publicSeasons/test");
  const old = mock.auth;
  act(() => mock.authError());
  expect(result.current.loading).toBe(false);
  expect(result.current.connection.accessError).toContain("retry access");
  act(() => result.current.connection.retryAccess());
  act(() => old({ uid: "stale" } as User));
  expect(result.current.connection.user).toBeNull();
  login();
  grant();
  loadPrivate();
  expect(result.current.connection.officer).toBe(true);
  expect(result.current.connection.accessError).toBe("");
});

it("saves only one roster document with its expected version and rejects stale or revoked writes", async () => {
  const { result } = renderHook(useFirestore);
  login();
  grant();
  loadPrivate();
  const value = {
    name: "Example",
    className: "Druid",
    role: "Healer",
    active: false,
    joinDate: "2026-10-01",
    leaveDate: "",
    notes: "Keep private",
  };
  const set = vi.fn();
  mock.transaction.mockImplementation(async (_db, callback) =>
    callback({
      get: async (ref: { path: string }) => ({ ...snap({ version: 3 }), ref }),
      set,
    }),
  );
  await act(async () =>
    expect(await result.current.connection.savePlayer("stable", value, 3)).toBe(
      4,
    ),
  );
  expect(set).toHaveBeenCalledTimes(1);
  expect(set.mock.calls[0][0].path).toBe("seasons/test/players/stable");
  expect(set.mock.calls[0][1]).toMatchObject({
    ...value,
    version: 4,
    updatedBy: "officer",
  });
  set.mockClear();
  await act(async () => {
    await expect(
      result.current.connection.savePlayer("stable", value, 2),
    ).rejects.toThrow("Another officer");
  });
  expect(set).not.toHaveBeenCalled();
  revoke();
  await expect(
    result.current.connection.savePlayer("stable", value, 3),
  ).rejects.toThrow("Officer access changed");
});

it("normalizes and saves one character mapping, rejects stale versions and missing relationships, and blocks revoked access", async () => {
  const { result } = renderHook(useFirestore);
  login();
  grant();
  loadPrivate();
  send(
    "seasons/test/players",
    {},
    { main: { name: "Example", active: false, version: 1 } },
  );
  const value = {
    name: " Étoile ",
    realm: " Example Realm ",
    region: " US ",
    playerId: "main",
  };
  const set = vi.fn();
  mock.transaction.mockImplementation(async (_db, callback) =>
    callback({
      get: async (ref: { path: string }) => ({ ...snap({ version: 3 }), ref }),
      set,
    }),
  );
  await act(async () =>
    expect(
      await result.current.connection.saveCharacter("stable", value, 3),
    ).toBe(4),
  );
  expect(set).toHaveBeenCalledTimes(1);
  expect(set.mock.calls[0][0].path).toBe("seasons/test/characters/stable");
  expect(set.mock.calls[0][1]).toMatchObject({
    name: "Étoile",
    realm: "Example Realm",
    region: "us",
    playerId: "main",
    version: 4,
    updatedBy: "officer",
  });
  set.mockClear();
  await act(async () => {
    await expect(
      result.current.connection.saveCharacter("stable", value, 2),
    ).rejects.toThrow("Another officer");
  });
  expect(set).not.toHaveBeenCalled();
  await expect(
    result.current.connection.saveCharacter(
      "stable",
      { ...value, playerId: "missing" },
      3,
    ),
  ).rejects.toThrow("Choose an existing main");
  send(
    "seasons/test/characters",
    {},
    {
      other: {
        name: "éTOILE",
        realm: "EXAMPLE REALM",
        region: "us",
        playerId: "main",
        version: 1,
      },
    },
  );
  await expect(
    result.current.connection.saveCharacter("new", value, 0),
  ).rejects.toThrow("already have a mapping");
  revoke();
  await expect(
    result.current.connection.saveCharacter("stable", value, 3),
  ).rejects.toThrow("Officer access changed");
});
