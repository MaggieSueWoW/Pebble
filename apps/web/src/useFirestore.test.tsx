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
  metadata: { hasPendingWrites: boolean };
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
  onAuthStateChanged: vi.fn((_auth, next) => {
    mock.auth = next;
    return vi.fn();
  }),
}));
vi.mock("firebase/firestore", () => ({
  doc: (_db: unknown, ...parts: string[]) => ({ path: parts.join("/") }),
  collection: (base: { path?: string }, ...parts: string[]) => ({
    path: [base.path, ...parts].filter(Boolean).join("/"),
  }),
  query: (ref: unknown) => ref,
  orderBy: vi.fn(),
  limit: vi.fn(),
  serverTimestamp: vi.fn(),
  onSnapshot: (
    ref: { path: string },
    next: Listener["next"],
    error: Listener["error"],
  ) => {
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
  metadata: { hasPendingWrites: false },
  docs: Object.entries(docs).map(([id, d]) => ({ id, data: () => d })),
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
const grant = (uid = "officer") => send(`officers/${uid}`, { enabled: true });
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
  vi.clearAllMocks();
  mock.getDoc.mockReset();
  mock.getDocs.mockReset();
  mock.setDoc.mockReset();
  mock.transaction.mockReset();
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

describe("officer access lifecycle", () => {
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
    expect(result.current.connection.error).toBe("Access denied");
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
    expect(result.current.connection.error).toBe("Membership unavailable");
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
    let save!: Promise<void>;
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

describe("run submission", () => {
  it("disables the actual Run button while submitting and until the run is observed", async () => {
    render(<Root />);
    login();
    grant();
    loadPrivate();
    send("seasons/test/runs");
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
    expect(screen.getByRole("status").textContent).toContain(
      "Waiting for the server to confirm",
    );
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
    expect(screen.getByRole("status").textContent).toContain(
      "Loading latest results",
    );
    expect(screen.queryByText("Results updated")).toBeNull();
    const pending = deferred<Snapshot>();
    mock.getDoc.mockReturnValueOnce(pending.promise);
    send("publicSeasons/test", { latestRunId: "new" });
    expect(screen.getByRole("status").textContent).toContain(
      "Loading latest results",
    );
    await act(async () => {
      pending.resolve(completed());
    });
    // Inputs saved after the captured snapshot still require another run.
    send("seasons/test/runs/new", {
      inputVersions: { "reports/aB3dE6gH9jK2mN5p": 2 },
    });
    expect(screen.getByRole("status").textContent).toContain(
      "Changes ready for the next run",
    );
    expect(screen.queryByText("Loading latest results…")).toBeNull();
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
