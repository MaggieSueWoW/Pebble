// @vitest-environment jsdom
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import App from "./App";
import { freshState, type Player } from "./model";
import type { LiveConnection } from "./useFirestore";

const alpha: Player = {
  id: "a",
  name: "Example Alpha",
  className: " Mage ",
  role: " Unknown ",
  color: "#b9c7d8",
  pre: 0,
  post: 0,
};
const beta: Player = {
  ...alpha,
  id: "b",
  name: "Example Beta",
  className: "",
  role: "Healer",
};
function connection(players: Player[] = []): LiveConnection {
  const saved = freshState();
  return {
    saved: {
      ...saved,
      inputs: { reports: [], adjustments: {}, timings: {} },
      results: { ...saved.results, players: [], nights: [] },
    },
    players,
    nights: [],
    user: null,
    officer: true,
    accessGeneration: 0,
    label: "Example season",
    online: false,
    active: null,
    versions: {},
    unprocessed: false,
    error: "",
    accessError: "",
    retryAccess: vi.fn(),
    busy: false,
    hasResults: false,
    submitting: false,
    queueReady: true,
    queueError: "",
    retryQueue: vi.fn(),
    resultsPending: false,
    resultsError: "",
    retryResults: vi.fn(),
    saveInputs: vi.fn().mockResolvedValue({}),
    requestRun: vi.fn(),
    signIn: vi.fn(),
    signOut: vi.fn(),
  };
}
const notes = () =>
  screen.getByRole("textbox", {
    name: "Availability notes",
  }) as HTMLTextAreaElement;
const editor = () =>
  screen.getByRole("region", { name: "Player availability editor" });
function overrides() {
  fireEvent.click(screen.getByRole("button", { name: "Overrides" }));
}
beforeEach(() => {
  vi.spyOn(window, "scrollTo").mockImplementation(() => {});
  HTMLDialogElement.prototype.close = vi.fn();
  HTMLDialogElement.prototype.showModal = vi.fn();
});
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

it("selects arriving players, keeps search independent and renders only known metadata", () => {
  const c = connection();
  const { rerender } = render(<App connection={c} />);
  overrides();
  expect(screen.getByText("No players in the roster yet.")).toBeTruthy();
  rerender(<App connection={{ ...c, players: [alpha, beta] }} />);
  expect(editor().textContent).toContain("Example Alpha");
  expect(editor().textContent).toContain("Mage");
  expect(editor().querySelector(".editor-heading")?.textContent).not.toMatch(
    /Unknown| · /,
  );
  fireEvent.change(screen.getByRole("textbox", { name: "Search players" }), {
    target: { value: "Beta" },
  });
  expect(editor().textContent).toContain("Example Alpha");
  fireEvent.click(screen.getByRole("button", { name: /Example Beta/ }));
  expect(editor().textContent).toContain("Healer");
  expect(editor().querySelector(".editor-heading")?.textContent).not.toContain(
    " · ",
  );
  fireEvent.change(screen.getByRole("textbox", { name: "Search players" }), {
    target: { value: "no match" },
  });
  expect(
    screen.getByText("No matching players. Try another search."),
  ).toBeTruthy();
  expect(editor().textContent).toContain("Example Beta");
  rerender(
    <App
      connection={{
        ...c,
        players: [{ ...alpha, className: "Unknown", role: "" }],
      }}
    />,
  );
  expect(editor().textContent).toContain("Example Alpha");
  expect(editor().querySelector(".editor-heading p")).toBeNull();
});

it("retains a removed player's draft until return or explicit cancellation", () => {
  const c = connection([alpha, beta]);
  const { rerender } = render(<App connection={c} />);
  overrides();
  fireEvent.change(notes(), { target: { value: "Retained draft" } });
  rerender(<App connection={{ ...c, players: [beta] }} />);
  expect(
    screen.getByText(/This player is no longer in the roster/),
  ).toBeTruthy();
  expect(screen.queryByRole("button", { name: "Save adjustment" })).toBeNull();
  expect(
    (screen.getByRole("button", { name: "Run Pebble" }) as HTMLButtonElement)
      .disabled,
  ).toBe(true);
  rerender(<App connection={c} />);
  expect(notes().value).toBe("Retained draft");
  rerender(<App connection={{ ...c, players: [beta] }} />);
  fireEvent.click(screen.getByRole("button", { name: "Cancel edits" }));
  expect(editor().textContent).toContain("Example Beta");
  expect(notes().value).toBe("");
  expect(c.saveInputs).not.toHaveBeenCalled();
});

it("waits for a removed player's pending save before selecting a replacement", async () => {
  let finish!: (value: Record<string, number>) => void;
  const c = connection([alpha, beta]);
  c.saveInputs = vi.fn(
    () =>
      new Promise<Record<string, number>>((resolve) => {
        finish = resolve;
      }),
  );
  const { rerender } = render(<App connection={c} />);
  overrides();
  fireEvent.change(notes(), { target: { value: "Submitted draft" } });
  fireEvent.click(screen.getByRole("button", { name: "Save adjustment" }));
  rerender(<App connection={{ ...c, players: [beta] }} />);
  expect(
    (screen.getByRole("button", { name: "Cancel edits" }) as HTMLButtonElement)
      .disabled,
  ).toBe(true);
  expect(screen.getByText(/Wait for the save to finish/)).toBeTruthy();
  await act(async () => finish({}));
  expect(editor().textContent).toContain("Example Beta");
  expect(notes().value).toBe("");
  expect(c.saveInputs).toHaveBeenCalledTimes(1);
});

it.each([true, false])(
  "shows published warning text on Results for officer=%s and hides empty warnings",
  (officer) => {
    const c = connection();
    c.officer = officer;
    c.hasResults = true;
    const warning = "2026-10-14: unknown characters: <Example>";
    c.saved.results.warnings = [
      warning,
      "2026-10-15: no Mythic encounters; no bench totals for this night",
    ];
    const { rerender } = render(<App connection={c} />);
    if (officer)
      fireEvent.click(screen.getByRole("button", { name: /^Results/ }));
    const region = screen.getByRole("region", { name: "Result warnings" });
    expect(region.querySelectorAll("li")).toHaveLength(2);
    expect(region.textContent).toContain(warning);
    expect(region.querySelector("example")).toBeNull();
    rerender(
      <App
        connection={{
          ...c,
          saved: { ...c.saved, results: { ...c.saved.results, warnings: [] } },
        }}
      />,
    );
    expect(
      screen.queryByRole("region", { name: "Result warnings" }),
    ).toBeNull();
    expect(c.requestRun).not.toHaveBeenCalled();
  },
);

function dates(
  c: LiveConnection,
  resultDates: string[],
  reportDates: string[] = [],
): LiveConnection {
  return {
    ...c,
    nights: resultDates,
    saved: {
      ...c.saved,
      inputs: {
        ...c.saved.inputs,
        reports: reportDates.map((night, i) => ({
          code: String.fromCharCode(65 + i).repeat(16),
          enabled: true,
          fetched: Boolean(night),
          title: "Example report",
          night,
        })),
      },
    },
  };
}
const raidDate = () =>
  screen.getByLabelText("Choose another raid date") as HTMLInputElement;
const raidPicker = () =>
  screen.getByRole("combobox", { name: "Raid night" }) as HTMLSelectElement;

it("defaults to the newest report/result date with one ordered deduplicated list", () => {
  const c = dates(
    connection([alpha]),
    ["2026-10-14", "2026-10-15"],
    ["", "2026-10-16", "2026-10-15"],
  );
  render(<App connection={c} />);
  overrides();
  expect(raidDate().value).toBe("2026-10-16");
  expect([...raidPicker().options].map((o) => o.value)).toEqual([
    "2026-10-16",
    "2026-10-15",
    "2026-10-14",
  ]);
  expect(c.saveInputs).not.toHaveBeenCalled();
});

it("starts on Pacific today, follows late metadata, then preserves an explicit custom date", () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-10-19T02:00:00Z"));
  try {
    const c = connection([alpha]);
    const { rerender } = render(<App connection={c} />);
    overrides();
    expect(raidDate().value).toBe("2026-10-18");
    rerender(<App connection={dates(c, [], ["2026-10-15"])} />);
    expect(raidDate().value).toBe("2026-10-15");
    rerender(<App connection={dates(c, ["2026-10-16"])} />);
    expect(raidDate().value).toBe("2026-10-16");
    fireEvent.input(raidDate(), { target: { value: "2026-10-12" } });
    rerender(<App connection={dates(c, ["2026-10-17"])} />);
    expect(raidDate().value).toBe("2026-10-12");
    expect(raidPicker().value).toBe("2026-10-12");
  } finally {
    vi.useRealTimers();
  }
});

it("keeps a picker choice after its metadata disappears", () => {
  const c = dates(connection([alpha]), ["2026-10-16", "2026-10-15"]);
  const { rerender } = render(<App connection={c} />);
  overrides();
  fireEvent.change(raidPicker(), { target: { value: "2026-10-15" } });
  rerender(<App connection={dates(c, ["2026-10-17"])} />);
  expect(raidDate().value).toBe("2026-10-15");
  expect([...raidPicker().options].map((o) => o.value)).toEqual([
    "2026-10-17",
    "2026-10-15",
  ]);
});

it("keeps the date of a draft after newer metadata and cancellation", () => {
  const c = dates(connection([alpha]), ["2026-10-14"]);
  const { rerender } = render(<App connection={c} />);
  overrides();
  fireEvent.change(notes(), { target: { value: "Night-bound draft" } });
  rerender(<App connection={dates(c, ["2026-10-15"])} />);
  expect(raidDate().value).toBe("2026-10-14");
  expect(notes().value).toBe("Night-bound draft");
  fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
  expect(raidDate().value).toBe("2026-10-14");
  expect(notes().value).toBe("");
});

it("keeps a pending save on its date until acknowledgement and afterwards", async () => {
  let finish!: (value: Record<string, number>) => void;
  const c = dates(connection([alpha]), ["2026-10-14"]);
  c.saveInputs = vi.fn(
    () =>
      new Promise<Record<string, number>>((resolve) => {
        finish = resolve;
      }),
  );
  const { rerender } = render(<App connection={c} />);
  overrides();
  fireEvent.change(notes(), { target: { value: "Save this night" } });
  fireEvent.click(screen.getByRole("button", { name: "Save adjustment" }));
  rerender(<App connection={dates(c, ["2026-10-15"])} />);
  expect(raidDate().value).toBe("2026-10-14");
  await act(async () => finish({ "availabilityOverrides/2026-10-14_a": 1 }));
  expect(raidDate().value).toBe("2026-10-14");
  expect(c.saveInputs).toHaveBeenCalledTimes(1);
});

it.each([
  ["auth/popup-blocked", /browser blocked Google sign-in/],
  ["auth/popup-closed-by-user", /sign-in was cancelled/],
  ["auth/unauthorized-domain", /authorize this hostname/],
  ["auth/network-request-failed", /Check your connection/],
])(
  "recovers from %s without displaying an SDK payload",
  async (code, message) => {
    const c = connection();
    c.officer = false;
    c.signIn = vi
      .fn()
      .mockRejectedValue(
        Object.assign(new Error("PRIVATE account-token payload"), { code }),
      );
    render(<App connection={c} />);
    fireEvent.click(screen.getByRole("button", { name: "Officer sign-in" }));
    await waitFor(() => expect(screen.getByText(message)).toBeTruthy());
    expect(screen.queryByText(/PRIVATE account-token/)).toBeNull();
    expect(
      (
        screen.getByRole("button", {
          name: "Officer sign-in",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(false);
  },
);
it("offers wrong-account switching and handles sign-out failure without losing the current session", async () => {
  const c = connection();
  c.officer = false;
  c.user = {
    uid: "example-member",
    displayName: "Example member",
  } as NonNullable<LiveConnection["user"]>;
  c.signOut = vi.fn().mockRejectedValue(new Error("PRIVATE sign-out payload"));
  render(<App connection={c} />);
  fireEvent.click(
    screen.getByRole("button", { name: "Switch Google account" }),
  );
  await waitFor(() => expect(c.signIn).toHaveBeenCalledOnce());
  fireEvent.click(screen.getByRole("button", { name: "Sign out" }));
  await waitFor(() =>
    expect(
      screen.getByText(/Couldn’t complete the account action/),
    ).toBeTruthy(),
  );
  expect(screen.getByText(/Firebase UID:/).textContent).toContain(
    "example-member",
  );
  expect(screen.queryByText(/PRIVATE sign-out/)).toBeNull();
});
