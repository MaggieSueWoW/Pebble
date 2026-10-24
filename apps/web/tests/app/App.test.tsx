// @vitest-environment jsdom
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import App from "../../src/app/App";
import { blankPlayer } from "../../src/features/roster/roster";
import { freshState } from "../../src/demo/model";
import {
  adjustmentKey,
  emptyAdjustment,
  type Player,
} from "../../src/shared/model";
import { defaultCalculation } from "../../src/features/settings/calculation";
import type { LiveConnection } from "../../src/data/useFirestore";
import { officerAction } from "../../src/features/officers/api";

vi.mock("../../src/features/officers/api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../src/features/officers/api")>()),
  officerAction: vi
    .fn()
    .mockResolvedValue({ invitations: [], invitation: null }),
}));

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
    roster: [],
    characters: [],
    ignoredCharacters: [],
    undoIgnoreCharacter: vi.fn().mockResolvedValue(undefined),
    ignoreCharacter: vi.fn().mockResolvedValue(undefined),
    saveCharacter: vi.fn(),
    deleteReport: vi.fn().mockResolvedValue(undefined),

    calculation: defaultCalculation(),
    saveCalculation: vi.fn(async (_value, version) => version + 1),
    savePlayer: vi.fn(),
    deletePlayer: vi.fn().mockResolvedValue(undefined),
    deleteCharacter: vi.fn().mockResolvedValue(undefined),
    swapMain: vi.fn().mockResolvedValue(undefined),
    nights: ["2026-10-15"],
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
    resultSources: [],
    resultDiagnostics: {},
    resultsPending: false,
    resultMetadataPending: false,
    resultsError: "",
    retryResults: vi.fn(),
    saveInputs: vi.fn().mockResolvedValue({}),
    requestRun: vi.fn(),
    signIn: vi.fn().mockResolvedValue(undefined),
    signOut: vi.fn().mockResolvedValue(undefined),
  };
}
const notes = () =>
  screen.getByRole("textbox", {
    name: "Availability notes",
  }) as HTMLTextAreaElement;
const editor = () =>
  screen.getByRole("region", { name: "Player availability editor" });

it("keeps connected sessions out of sample storage and submits runs through the live connection", async () => {
  const live = connection();
  const read = vi.spyOn(Storage.prototype, "getItem");
  const write = vi.spyOn(Storage.prototype, "setItem");
  render(<App connection={live} />);
  expect(screen.queryByText("Prototype controls")).toBeNull();
  await act(async () => {
    fireEvent.click(screen.getByRole("button", { name: "Run Pebble" }));
  });
  expect(live.requestRun).toHaveBeenCalledOnce();
  expect(read).not.toHaveBeenCalled();
  expect(write).not.toHaveBeenCalled();
});

function overrides() {
  fireEvent.click(screen.getByRole("button", { name: "Overrides" }));
  const summary = document.querySelector(".availability-notes > summary");
  if (summary) fireEvent.click(summary);
}
beforeEach(() => {
  window.history.replaceState(null, "", "/");
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
  rerender(<App connection={{ ...c, players: [beta, alpha] }} />);
  expect(
    [...document.querySelectorAll(".player-item strong")].map(
      (el) => el.textContent,
    ),
  ).toEqual(["Example Alpha", "Example Beta"]);
  expect(editor().textContent).toContain("Example Alpha");
  expect(editor().textContent).not.toContain("Mage");
  const search = screen.getByRole("textbox", { name: "Search players" });
  fireEvent.change(search, { target: { value: "mage" } });
  expect(screen.getByRole("button", { name: /Example Alpha/ })).toBeTruthy();
  expect(screen.queryByRole("button", { name: /Example Beta/ })).toBeNull();
  expect(
    within(editor()).getByRole("heading", { level: 2 }).textContent,
  ).not.toMatch(/Unknown| · /);
  fireEvent.change(screen.getByRole("textbox", { name: "Search players" }), {
    target: { value: "Beta" },
  });
  expect(editor().textContent).toContain("Example Alpha");
  fireEvent.click(screen.getByRole("button", { name: /Example Beta/ }));
  expect(editor().textContent).toContain("Example Beta");
  expect(
    within(editor()).getByRole("heading", { level: 2 }).textContent,
  ).not.toContain(" · ");
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
  expect(
    screen.queryByRole("button", { name: "Save availability" }),
  ).toBeNull();
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
  fireEvent.click(screen.getByRole("button", { name: "Save availability" }));
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
    c.nights = ["2026-10-14", "2026-10-15"];
    c.hasResults = true;
    const warning = "2026-10-14: unknown characters: <Example>";
    c.saved.results.warnings = [
      warning,
      "2026-10-15: no Mythic encounters; no bench totals for this night",
    ];
    const { rerender } = render(<App connection={c} />);
    if (officer)
      fireEvent.click(screen.getByRole("button", { name: /^Results/ }));
    if (officer) {
      fireEvent.click(
        screen.getAllByRole("button", {
          name: /^Review (warnings for|other warnings)/,
        })[0],
      );
      fireEvent.change(screen.getByRole("combobox", { name: "Raid night" }), {
        target: { value: "2026-10-14" },
      });
    }
    const region = screen.getByRole("region", { name: "Result warnings" });
    expect(region.querySelectorAll("li")).toHaveLength(officer ? 1 : 2);
    expect(region.textContent).toContain(
      officer ? "Unknown players: <Example>" : warning,
    );
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
  expect(raidPicker().value).toBe("2026-10-16");
  expect([...raidPicker().options].map((o) => o.value)).toEqual([
    "2026-10-16",
    "2026-10-15",
    "2026-10-14",
  ]);
  expect(c.saveInputs).not.toHaveBeenCalled();
});

it("waits for logged nights and preserves a selected known night", () => {
  const c = dates(connection([alpha]), []);
  const { rerender } = render(<App connection={c} />);
  overrides();
  expect(screen.getByText(/No raid nights identified yet/)).toBeTruthy();
  expect(
    screen.queryByRole("region", { name: "Player availability editor" }),
  ).toBeNull();
  expect(screen.queryByLabelText("Choose another raid date")).toBeNull();
  rerender(<App connection={dates(c, [], ["2026-10-15"])} />);
  expect(raidPicker().value).toBe("2026-10-15");
  rerender(<App connection={dates(c, ["2026-10-16", "2026-10-15"])} />);
  expect(raidPicker().value).toBe("2026-10-16");
  fireEvent.change(raidPicker(), { target: { value: "2026-10-15" } });
  rerender(<App connection={dates(c, ["2026-10-17", "2026-10-15"])} />);
  expect(raidPicker().value).toBe("2026-10-15");
});

it("moves a clean selection to a logged night when its metadata disappears", () => {
  const c = dates(connection([alpha]), ["2026-10-16", "2026-10-15"]);
  const { rerender } = render(<App connection={c} />);
  overrides();
  fireEvent.change(raidPicker(), { target: { value: "2026-10-15" } });
  rerender(<App connection={dates(c, ["2026-10-17"])} />);
  expect(raidPicker().value).toBe("2026-10-17");
  expect([...raidPicker().options].map((o) => o.value)).toEqual(["2026-10-17"]);
});

it("blocks a draft whose logged night disappears until cancellation", () => {
  const c = dates(connection([alpha]), ["2026-10-14"]);
  const { rerender } = render(<App connection={c} />);
  overrides();
  fireEvent.change(notes(), { target: { value: "Night-bound draft" } });
  rerender(<App connection={dates(c, ["2026-10-15"])} />);
  expect(
    screen
      .getByRole("button", { name: "Save availability" })
      .hasAttribute("disabled"),
  ).toBe(true);
  expect(notes().value).toBe("Night-bound draft");
  fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
  expect(raidPicker().value).toBe("2026-10-15");
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
  fireEvent.click(screen.getByRole("button", { name: "Save availability" }));
  rerender(<App connection={dates(c, ["2026-10-15"])} />);
  expect(notes().value).toBe("Save this night");
  await act(async () => finish({ "availabilityOverrides/2026-10-14_a": 1 }));
  expect(raidPicker().value).toBe("2026-10-15");
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
it("handles sign-out failure without losing the current session", async () => {
  const c = connection();
  c.officer = false;
  c.user = {
    uid: "example-member",
    displayName: "Example member",
  } as NonNullable<LiveConnection["user"]>;
  c.signOut = vi.fn().mockRejectedValue(new Error("PRIVATE sign-out payload"));
  render(<App connection={c} />);
  expect(screen.queryByRole("button", { name: "Officer sign-in" })).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "Sign out" }));
  await waitFor(() =>
    expect(
      screen.getByText(/Couldn’t complete the account action/),
    ).toBeTruthy(),
  );
  expect(screen.getByText(/Account ID:/).textContent).toContain(
    "example-member",
  );
  expect(screen.queryByText(/PRIVATE sign-out/)).toBeNull();
});
it("keeps public sign-in and explains non-officer access with collapsed account details", async () => {
  const c = connection();
  c.officer = false;
  c.hasResults = true;
  const { rerender } = render(<App connection={c} />);
  expect(
    screen.queryByRole("region", { name: "Officer access not enabled" }),
  ).toBeNull();
  await act(async () =>
    fireEvent.click(screen.getByRole("button", { name: "Officer sign-in" })),
  );
  expect(c.signIn).toHaveBeenCalledTimes(1);
  c.user = {
    uid: "example-account",
    displayName: "Example Reader",
  } as NonNullable<LiveConnection["user"]>;
  rerender(<App connection={{ ...c }} />);
  const region = screen.getByRole("region", {
    name: "Officer access not enabled",
  });
  expect(region.textContent).toContain("You can still read completed results");
  const details = region.querySelector("details")!;
  expect(details.open).toBe(false);
  fireEvent.click(screen.getByText("Account details", { exact: true }));
  expect(details.open).toBe(true);
  expect(details.querySelector("code")?.textContent).toBe("example-account");
  expect(screen.queryByRole("button", { name: /^Reports/ })).toBeNull();
  expect(screen.queryByRole("button", { name: "Overrides" })).toBeNull();
  expect(screen.queryByRole("button", { name: "Run Pebble" })).toBeNull();
  await act(async () =>
    fireEvent.click(screen.getByRole("button", { name: "Sign out" })),
  );
  expect(c.signOut).toHaveBeenCalledTimes(1);
  expect(c.saveInputs).not.toHaveBeenCalled();
  rerender(<App connection={{ ...c, officer: true }} />);
  expect(
    screen.queryByRole("region", { name: "Officer access not enabled" }),
  ).toBeNull();
  expect(screen.getByRole("button", { name: /^Reports/ })).toBeTruthy();
});

it("announces an account-switch failure without attempting another sign-in", async () => {
  const c = connection();
  c.officer = false;
  c.hasResults = true;
  c.user = { uid: "example-account" } as NonNullable<LiveConnection["user"]>;
  c.signOut = vi.fn().mockRejectedValue(new Error("Sign-out unavailable"));
  render(<App connection={c} />);
  await act(async () =>
    fireEvent.click(screen.getByRole("button", { name: "Sign out" })),
  );
  expect(screen.getByRole("alert").textContent).toContain(
    "Couldn’t complete the account action",
  );
  expect(c.signIn).not.toHaveBeenCalled();
});

it.each([true, false])(
  "explains missing completed results for officer=%s and suppresses it during loading/errors",
  (officer) => {
    const c = connection();
    c.officer = officer;
    const { rerender } = render(<App connection={c} />);
    if (officer)
      fireEvent.click(screen.getByRole("button", { name: /^Results/ }));
    expect(screen.getByText(/No completed results yet/).textContent).toContain(
      officer ? "Add or include reports" : "after an officer runs Pebble",
    );
    expect(screen.queryByRole("button", { name: "Manage reports" })).toBeNull();
    expect(
      screen.queryByRole("heading", { name: "No player time for this period" }),
    ).toBeNull();
    rerender(<App connection={{ ...c, resultsPending: true }} />);
    expect(screen.queryByText(/No completed results yet/)).toBeNull();
    rerender(
      <App connection={{ ...c, resultsError: "Result reads unavailable" }} />,
    );
    expect(screen.queryByText(/No completed results yet/)).toBeNull();
    expect(screen.getByText("Result reads unavailable")).toBeTruthy();
  },
);

it.each([true, false])(
  "explains empty completed periods for officer=%s without claiming no result exists",
  (officer) => {
    const c = connection();
    c.officer = officer;
    c.hasResults = true;
    render(<App connection={c} />);
    if (officer)
      fireEvent.click(screen.getByRole("button", { name: /^Results/ }));
    expect(
      screen.getByRole("heading", { name: "No player time for this period" }),
    ).toBeTruthy();
    expect(screen.queryByText(/No completed results yet/)).toBeNull();
    expect(
      screen.getByText(
        officer
          ? "Use Reports in the sidebar to review the included logs."
          : "No player time was recorded for this period in the completed results.",
      ),
    ).toBeTruthy();
    if (officer) {
      fireEvent.click(
        within(
          screen.getByRole("navigation", { name: "Main navigation" }),
        ).getByRole("button", {
          name: /^Reports/,
        }),
      );
      expect(
        screen.getByRole("textbox", { name: "Warcraft Logs reports" }),
      ).toBeTruthy();
    } else
      expect(
        screen.queryByRole("button", { name: "Manage reports" }),
      ).toBeNull();
    expect(c.saveInputs).not.toHaveBeenCalled();
    expect(c.requestRun).not.toHaveBeenCalled();
  },
);

it("explains empty recent history without hiding active progress or previous results", () => {
  const c = connection();
  c.hasResults = true;
  c.saved.history = [];
  c.active = { id: "queued", status: "queued", stage: 0 };
  const { rerender } = render(<App connection={c} />);
  expect(screen.getByText("No completed runs in recent history.")).toBeTruthy();
  expect(
    (screen.getByRole("button", { name: "Run queued" }) as HTMLButtonElement)
      .disabled,
  ).toBe(true);
  expect(
    screen.getByRole("status", { name: "Run status" }).textContent,
  ).toContain("Your last completed results stay visible");
  rerender(
    <App
      connection={{
        ...c,
        saved: {
          ...c.saved,
          history: [
            {
              id: "done",
              status: "succeeded",
              at: "2026-10-19T02:00:00Z",
              message: "Completed fixture",
            },
          ],
        },
      }}
    />,
  );
  expect(screen.queryByText("No completed runs in recent history.")).toBeNull();
  expect(screen.getByText("Completed fixture")).toBeTruthy();
});

it("places timing review on Review Night and availability on Overrides", () => {
  render(<App connection={connection([alpha])} />);
  overrides();
  expect(
    screen.getByRole("region", { name: "Player availability editor" }),
  ).toBeTruthy();
  expect(
    screen.queryByRole("region", { name: "Raid timing editor" }),
  ).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "Review Night" }));
  const disclosure = screen
    .getByText("Times look wrong? Adjust timing")
    .closest("details")!;
  expect(disclosure.open).toBe(false);
  fireEvent.click(screen.getByText("Times look wrong? Adjust timing"));
  expect(disclosure.open).toBe(true);
  expect(
    screen.getByRole("region", { name: "Raid timing editor" }),
  ).toBeTruthy();
  expect(
    screen.queryByRole("region", { name: "Player availability editor" }),
  ).toBeNull();
});

it.each([true, false])(
  "shows warning source links only to officers (officer=%s)",
  (officer) => {
    const c = connection();
    c.officer = officer;
    c.hasResults = true;
    c.saved.results.warnings = ["Recorded warning"];
    c.resultSources = [
      {
        code: "AAAAAAAAAAAAAAAA",
        title: "Example source report",
        warningIndexes: [0],
      },
      {
        code: "BBBBBBBBBBBBBBBB",
        title: "Unrelated report",
        warningIndexes: [1],
      },
    ];
    const { rerender } = render(<App connection={c} />);
    if (officer)
      fireEvent.click(screen.getByRole("button", { name: /^Results/ }));
    if (officer)
      fireEvent.click(
        screen.getAllByRole("button", {
          name: /^Review (warnings for|other warnings)/,
        })[0],
      );
    expect(
      screen.getByRole("region", { name: "Result warnings" }),
    ).toBeTruthy();
    if (officer) {
      const source = screen.getByRole("link", {
        name: "Example source report",
      });
      expect(source.getAttribute("href")).toBe(
        "https://www.warcraftlogs.com/reports/AAAAAAAAAAAAAAAA",
      );
      expect(source.getAttribute("target")).toBe("_blank");
      expect(
        screen.getByRole("list", { name: "Source reports for this warning" }),
      ).toBeTruthy();
    } else
      expect(
        screen.queryByRole("link", { name: "Example source report" }),
      ).toBeNull();
    expect(screen.queryByRole("link", { name: "Unrelated report" })).toBeNull();
    rerender(<App connection={{ ...c, resultSources: [] }} />);
    expect(
      screen.queryByRole("list", { name: "Source reports for this warning" }),
    ).toBeNull();
    expect(screen.getByText("Recorded warning")).toBeTruthy();
  },
);

it("links both result disclosure controls to stable hidden detail content", () => {
  const c = connection([alpha]);
  c.hasResults = true;
  c.saved.results.players = [
    {
      id: alpha.id,
      name: alpha.name,
      className: "Mage",
      played: 60,
      bench: 30,
      preBench: 10,
      postBench: 20,
      overridden: false,
      nights: 1,
    },
  ];
  render(<App connection={c} />);
  fireEvent.click(screen.getByRole("button", { name: /^Results/ }));
  const detail = screen.getByRole("button", {
    name: "Details for Example Alpha",
  });
  const name = screen.getByRole("button", {
    name: "Example Alpha",
  });
  const id = detail.getAttribute("aria-controls")!;
  expect(name.getAttribute("aria-controls")).toBe(id);
  expect(document.getElementById(id)?.closest("tr")?.hidden).toBe(true);
  fireEvent.click(detail);
  expect(
    screen.getByRole("region", {
      name: "Availability breakdown for Example Alpha",
    }).id,
  ).toBe(id);
  fireEvent.click(name);
  expect(document.getElementById(id)?.closest("tr")?.hidden).toBe(true);
  expect(detail.getAttribute("aria-expanded")).toBe("false");
  const row = detail.closest("tr")!;
  fireEvent.click(row.querySelector(".bench-value")!);
  expect(detail.getAttribute("aria-expanded")).toBe("true");
  fireEvent.click(row.querySelector(".time-bar")!);
  expect(detail.getAttribute("aria-expanded")).toBe("false");
});

it.each(["results", "queue"] as const)(
  "recovers focus after a disappearing %s retry without putting actions in Run status",
  async (kind) => {
    const c = connection();
    if (kind === "results") c.resultsError = "Result read unavailable";
    else {
      c.queueReady = false;
      c.queueError = "Queue read unavailable";
    }
    const { rerender } = render(<App connection={c} />);
    const retry = () =>
      rerender(
        <App
          connection={{
            ...c,
            resultsError: "",
            resultsPending: kind === "results",
            queueError: "",
            queueReady: true,
          }}
        />,
      );
    c.retryResults = retry;
    c.retryQueue = retry;
    rerender(<App connection={{ ...c }} />);
    const button = screen.getByRole("button", {
      name: kind === "results" ? "Retry loading results" : "Retry run queue",
    });
    button.focus();
    fireEvent.click(button);
    await waitFor(() =>
      expect(document.activeElement).toBe(
        document.getElementById("page-title"),
      ),
    );
    const status = screen.getByRole("status", { name: "Run status" });
    expect(status.getAttribute("aria-atomic")).toBe("true");
    expect(status.querySelector("button")).toBeNull();
  },
);

it("connects report/availability validation and names the discard dialog", () => {
  render(<App connection={connection([alpha])} />);
  const input = screen.getByRole("textbox", { name: "Warcraft Logs reports" });
  fireEvent.change(input, { target: { value: "invalid" } });
  expect(input.getAttribute("aria-describedby")).toContain("report-validation");
  expect(document.getElementById("report-validation")?.textContent).toContain(
    "invalid",
  );
  fireEvent.click(screen.getByRole("button", { name: /^Results/ }));
  const dialog = document.querySelector("dialog")!;
  expect(
    document.getElementById(dialog.getAttribute("aria-labelledby")!)
      ?.textContent,
  ).toBe("Keep your changes?");
  expect(
    document.getElementById(dialog.getAttribute("aria-describedby")!)
      ?.textContent,
  ).toContain("unsaved edits");
  fireEvent.click(
    screen.getByRole("button", { name: "Discard edits", hidden: true }),
  );
  overrides();
  fireEvent.change(screen.getByRole("combobox", { name: "Before break" }), {
    target: { value: "partial" },
  });
  const minutes = screen.getByRole("spinbutton", {
    name: "Before break available minutes",
  });
  fireEvent.change(minutes, { target: { value: "0" } });
  expect(minutes.getAttribute("aria-describedby")).toContain(
    "availability-validation",
  );
  expect(
    document.getElementById("availability-validation")?.getAttribute("role"),
  ).toBe("alert");
});

it("offers season and night results and falls back from a removed weekly link", () => {
  history.replaceState(null, "", "/?view=results&period=week:2026-10-13");
  const c = connection([alpha]);
  c.hasResults = true;
  c.saved.results.weeks = [{ week: "2026-10-13", players: [] }];
  render(<App connection={c} />);
  const period = screen.getByRole("combobox", {
    name: "Result period",
  }) as HTMLSelectElement;
  expect([...period.options].map((option) => option.value)).toEqual(["season"]);
  expect(period.value).toBe("season");
  expect(location.search).toContain("period=season");
  expect(screen.getByText(/This result period is not available/)).toBeTruthy();
  expect(
    within(screen.getByRole("complementary")).getByRole("button", {
      name: "Run Pebble",
    }),
  ).toBeTruthy();
  expect(
    screen.getByRole("status", { name: "Run status" }).querySelector("time")
      ?.dateTime,
  ).toBe(c.saved.results.completedAt);
});

it("retains a linked result selection through pending reads and failures until matching results arrive", () => {
  history.replaceState(null, "", "/?view=results&period=2026-10-15&player=a");
  const c = connection([alpha]);
  c.resultsPending = true;
  const { rerender } = render(<App connection={c} />);
  expect(location.search).toContain("period=2026-10-15");
  rerender(
    <App
      connection={{
        ...c,
        resultsPending: false,
        resultsError: "Read unavailable",
      }}
    />,
  );
  expect(location.search).toContain("player=a");
  const row = {
    id: "a",
    name: alpha.name,
    played: 60,
    bench: 30,
    prePlayed: 10,
    postPlayed: 50,
    preBench: 10,
    postBench: 20,
    overridden: false,
    nights: 1,
  };
  c.saved.results = {
    ...c.saved.results,
    nights: [
      {
        night: "2026-10-15",
        duration: 90,
        reports: 1,
        timing: { start: "", breakStart: "", breakEnd: "", end: "", notes: "" },
        players: [row],
      },
    ],
    players: [row],
  };
  rerender(
    <App connection={{ ...c, hasResults: true, resultsPending: false }} />,
  );
  expect(
    (
      screen.getByRole("combobox", {
        name: "Result period",
      }) as HTMLSelectElement
    ).value,
  ).toBe("2026-10-15");
  expect(
    screen.getByRole("region", {
      name: `Availability breakdown for ${alpha.name}`,
    }),
  ).toBeTruthy();
  expect(screen.queryByText(/This result period is not available/)).toBeNull();
});

it("explains missing linked periods and players after result readiness", () => {
  history.replaceState(
    null,
    "",
    "/?view=results&period=2026-01-01&player=missing",
  );
  const c = connection();
  const { unmount } = render(<App connection={c} />);
  expect(screen.getByText(/This result period is not available/)).toBeTruthy();
  expect(new URLSearchParams(location.search).get("period")).toBe("season");
  expect(new URLSearchParams(location.search).has("player")).toBe(false);
  unmount();
  history.replaceState(null, "", "/?view=results&period=season&player=missing");
  render(<App connection={c} />);
  expect(screen.getByText(/This player is not available/)).toBeTruthy();
  expect(new URLSearchParams(location.search).has("player")).toBe(false);
});

it("exposes published attendance for zero-time players outside ranked rows and only in the season view", () => {
  history.replaceState(null, "", "/?view=results&period=season&player=absent");
  const c = connection();
  c.officer = false;
  c.hasResults = true;
  const absent = {
    id: "absent",
    name: "Absent",
    played: 0,
    bench: 0,
    preBench: 0,
    postBench: 0,
    overridden: false,
    nights: 0,
    attendance: {
      played: 0,
      bench: 0,
      possible: 120,
      rate: 0,
      weeks: [{ week: "2026-10-13", status: "O" }],
    },
  };
  c.saved.results.players = [
    absent,
    { ...absent, id: "legacy", name: "Legacy", attendance: undefined },
  ];
  c.saved.results.nights = [
    {
      night: "2026-10-15",
      duration: 120,
      timing: { start: "", breakStart: "", breakEnd: "", end: "", notes: "" },
      reports: 1,
      players: [],
    },
  ];
  render(<App connection={c} />);
  const additional = screen.getByRole("region", {
    name: "Additional attendance",
  });
  const disclosure = within(additional).getByText("Attendance for Absent");
  expect(disclosure.closest("details")?.open).toBe(true);
  expect(new URLSearchParams(location.search).get("player")).toBe("absent");
  expect(screen.queryByText(/This player is not available/)).toBeNull();
  expect(within(additional).getByText("0%", { exact: true })).toBeTruthy();
  expect(within(additional).queryByText("Attendance for Legacy")).toBeNull();
  expect(
    screen.queryByRole("button", { name: "Details for Absent" }),
  ).toBeNull();
  fireEvent.change(screen.getByRole("combobox", { name: "Result period" }), {
    target: { value: "2026-10-15" },
  });
  expect(
    screen.queryByRole("region", { name: "Additional attendance" }),
  ).toBeNull();
});

it("shows nightly audit values and highlights only the recorded override half", () => {
  const c = connection([alpha, beta]);
  c.saved.results.nights = [
    {
      night: "2026-10-15",
      duration: 180,
      reports: 1,
      timing: { start: "", breakStart: "", breakEnd: "", end: "", notes: "" },
      players: [
        {
          id: alpha.id,
          played: 80,
          bench: 10,
          preBench: 10,
          postBench: 0,
          prePlayed: 0,
          postPlayed: 80,
          overridden: true,
          nights: 1,
          availability: {
            pre: { available: true, mode: "partial" },
            post: { available: true, mode: "automatic" },
            source: "override",
          },
        },
      ],
    },
  ];
  const { rerender } = render(<App connection={c} />);
  overrides();
  const row = screen.getByRole("button", { name: /Example Alpha/ });
  expect(row.textContent).not.toContain("Automatic");
  expect(within(row).getByText("Override")).toBeTruthy();
  const values = [...row.querySelectorAll(".override-minute")];
  expect(values.map((value) => value.textContent)).toEqual(["10", "-", "10"]);
  expect(
    values.map((value) => value.classList.contains("override-affected")),
  ).toEqual([true, false, true]);
  const uncalculated = screen.getByRole("button", { name: /Example Beta/ });
  expect(uncalculated.querySelectorAll(".override-affected").length).toBe(0);
  expect(
    [...uncalculated.querySelectorAll(".override-minute")].map(
      (value) => value.textContent,
    ),
  ).toEqual(["—", "—", "—"]);
  expect(within(editor()).getByText("1h 20m")).toBeTruthy();
  // A saved reset takes precedence over stale published override markers.
  rerender(
    <App
      connection={{
        ...c,
        saved: {
          ...c.saved,
          inputs: {
            ...c.saved.inputs,
            adjustments: {
              [adjustmentKey("2026-10-15", alpha.id)]: emptyAdjustment(),
            },
          },
        },
      }}
    />,
  );
  expect(screen.getByText("0 players adjusted")).toBeTruthy();
  expect(within(row).queryByText("Override")).toBeNull();
  expect(row.querySelectorAll(".override-affected").length).toBe(0);
  fireEvent.click(screen.getByRole("checkbox", { name: "Overrides only" }));
  expect(screen.getByText("No matching availability overrides.")).toBeTruthy();
  expect(editor().textContent).toContain(alpha.name);
});

it("lists mains with alts and scopes a new alt to the clicked main", () => {
  const c = connection([alpha, beta]);
  c.roster = [
    { ...blankPlayer(), id: "b", name: beta.name, version: 1 },
    { ...blankPlayer(), id: "a", name: alpha.name, version: 1 },
  ];
  c.characters = [
    {
      id: "alt",
      name: "Existingalt",
      realm: "Example realm",
      region: "us",
      playerId: "a",
      version: 1,
    },
  ];
  c.characters.push({ ...c.characters[0], id: "alt2", name: "Secondalt" });
  render(<App connection={c} />);
  fireEvent.click(screen.getByRole("button", { name: "Roster" }));
  expect(
    [...document.querySelectorAll(".roster-main-button strong")].map(
      (el) => el.textContent,
    ),
  ).toEqual([alpha.name, beta.name]);
  expect(
    screen.getByRole("button", {
      name: "Edit alt Existingalt for Example Alpha",
    }),
  ).toBeTruthy();
  expect(document.querySelector(".roster-alts")?.textContent).toBe(
    "Existingalt, Secondalt",
  );
  expect(screen.queryByRole("button", { name: /Add alt for/ })).toBeNull();
  fireEvent.change(screen.getByRole("textbox", { name: "Search roster" }), {
    target: { value: "Existingalt" },
  });
  expect(
    screen.queryByRole("button", { name: "Edit Example Beta" }),
  ).toBeNull();
  fireEvent.change(screen.getByRole("textbox", { name: "Search roster" }), {
    target: { value: "" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Edit Example Beta" }));
  fireEvent.click(screen.getByRole("button", { name: "Add alt" }));
  expect(screen.queryByRole("combobox", { name: "Mapped main" })).toBeNull();
  expect(c.saveCharacter).not.toHaveBeenCalled();
  fireEvent.change(screen.getByRole("textbox", { name: "Character name" }), {
    target: { value: "Draftalt" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Edit Example Alpha" }));
  expect(
    (
      screen.getByRole("textbox", {
        name: "Character name",
      }) as HTMLInputElement
    ).value,
  ).toBe("Draftalt");
  fireEvent.click(
    screen.getByRole("button", { name: "Keep editing", hidden: true }),
  );
  fireEvent.click(screen.getByRole("button", { name: "Cancel alt edits" }));
  fireEvent.click(screen.getByRole("button", { name: "Edit Example Alpha" }));
  expect(screen.queryByRole("combobox", { name: "Role" })).toBeNull();
  expect(
    (screen.getByRole("textbox", { name: "Main name" }) as HTMLInputElement)
      .value,
  ).toBe(alpha.name);
});

it("toggles Results column sorting while keeping details on the selected player", () => {
  const c = connection([alpha, beta]);
  c.hasResults = true;
  c.saved.results.players = [
    {
      id: alpha.id,
      name: alpha.name,
      played: 30,
      bench: 10,
      preBench: 10,
      postBench: 0,
      overridden: false,
      nights: 1,
    },
    {
      id: beta.id,
      name: beta.name,
      played: 10,
      bench: 20,
      preBench: 20,
      postBench: 0,
      overridden: false,
      nights: 1,
    },
  ];
  render(<App connection={c} />);
  fireEvent.click(screen.getByRole("button", { name: "Results" }));
  const names = () =>
    [...document.querySelectorAll(".result-name strong")].map(
      (el) => el.textContent,
    );
  fireEvent.click(
    screen.getByRole("button", { name: "Details for Example Alpha" }),
  );
  const columns = screen.getAllByRole("columnheader");
  expect(within(columns[0]).queryByRole("button")).toBeNull();
  const benchSort = screen.getByRole("button", { name: "Bench" });
  expect(benchSort.closest("th")?.getAttribute("aria-sort")).toBe("ascending");
  fireEvent.click(benchSort);
  expect(benchSort.closest("th")?.getAttribute("aria-sort")).toBe("descending");
  expect(names()).toEqual([beta.name, alpha.name]);
  const nameSort = screen.getByRole("button", { name: "Player" });
  fireEvent.click(nameSort);
  expect(names()).toEqual([alpha.name, beta.name]);
  expect(nameSort.closest("th")?.getAttribute("aria-sort")).toBe("ascending");
  fireEvent.click(nameSort);
  expect(names()).toEqual([beta.name, alpha.name]);
  expect(nameSort.closest("th")?.getAttribute("aria-sort")).toBe("descending");
  fireEvent.click(screen.getByRole("button", { name: "Bench" }));
  expect(names()).toEqual([alpha.name, beta.name]);
  fireEvent.click(screen.getByRole("button", { name: "Played" }));
  expect(names()).toEqual([alpha.name, beta.name]);
  fireEvent.click(screen.getByRole("button", { name: "Played %" }));
  expect(names()).toEqual([alpha.name, beta.name]);
  expect(
    document.querySelector('.result-name[aria-expanded="true"]')?.textContent,
  ).toBe(alpha.name);
});

it("sorts each Overrides audit column with an explicit active direction and missing values last", () => {
  const c = connection([
    beta,
    { ...alpha, id: "missing", name: "Missing" },
    alpha,
  ]);
  c.saved.results.nights = [
    {
      night: "2026-10-15",
      duration: 180,
      reports: 1,
      timing: { start: "", breakStart: "", breakEnd: "", end: "", notes: "" },
      players: [
        {
          id: alpha.id,
          played: 90,
          bench: 20,
          preBench: 0,
          postBench: 20,
          prePlayed: 80,
          postPlayed: 10,
          nights: 1,
          overridden: false,
        },
        {
          id: beta.id,
          played: 80,
          bench: 40,
          preBench: 30,
          postBench: 10,
          prePlayed: 20,
          postPlayed: 60,
          nights: 1,
          overridden: false,
        },
      ],
    },
  ];
  render(<App connection={c} />);
  overrides();
  const order = () =>
    [...document.querySelectorAll(".override-player-row strong")].map(
      (el) => el.textContent,
    );
  expect(order()).toEqual([alpha.name, beta.name, "Missing"]);
  const initial = screen.getByRole("button", {
    name: "Sort by Player, ascending",
  });
  expect(initial.getAttribute("aria-pressed")).toBe("true");
  fireEvent.click(initial);
  expect(order()).toEqual(["Missing", beta.name, alpha.name]);
  expect(
    screen
      .getByRole("button", { name: "Sort by Player, descending" })
      .getAttribute("aria-pressed"),
  ).toBe("true");
  for (const [label, first, second, direction] of [
    ["Bench before break", alpha.name, beta.name, "ascending"],
    ["Bench after break", beta.name, alpha.name, "ascending"],
    ["Total bench", alpha.name, beta.name, "ascending"],
  ]) {
    fireEvent.click(screen.getByRole("button", { name: `Sort by ${label}` }));
    expect(order()).toEqual([first, second, "Missing"]);
    const active = screen.getByRole("button", {
      name: `Sort by ${label}, ${direction}`,
    });
    expect(active.getAttribute("aria-pressed")).toBe("true");
    expect(
      document.querySelectorAll('.result-sort-button[aria-pressed="true"]')
        .length,
    ).toBe(1);
    fireEvent.click(active);
    expect(order()).toEqual([second, first, "Missing"]);
  }
  // Reordering and filtering retain the selected player and editor.
  expect(editor().textContent).toContain(alpha.name);
  fireEvent.change(screen.getByRole("textbox", { name: "Search players" }), {
    target: { value: "Mage" },
  });
  expect(order()).toEqual([alpha.name, "Missing"]);
  expect(editor().textContent).toContain(alpha.name);
});

it("keeps failed-run status visible on every page after compacting the headers", () => {
  const c = connection([alpha]);
  c.hasResults = true;
  c.saved.history = [
    {
      id: "failed",
      at: "2026-10-15T04:00:00Z",
      status: "failed",
      message: "Synthetic failure",
    },
  ];
  render(<App connection={c} />);
  for (const page of ["Results", "Overrides", "Roster", "Settings"]) {
    fireEvent.click(screen.getByRole("button", { name: page }));
    expect(
      screen.getByRole("status", { name: "Run status" }).textContent,
    ).toContain("Last run didn’t finish");
  }
});

it("guards roster navigation, sign-out and reload during a pending mutation", () => {
  const c = connection([alpha, beta]);
  c.roster = [alpha, beta].map((player) => ({
    ...blankPlayer(),
    id: player.id,
    name: player.name,
    version: 1,
  }));
  c.user = { uid: "officer", displayName: "Example Officer" } as NonNullable<
    LiveConnection["user"]
  >;
  const { rerender } = render(<App connection={c} />);
  fireEvent.click(screen.getByRole("button", { name: "Roster" }));
  fireEvent.click(screen.getByRole("button", { name: `Edit ${alpha.name}` }));
  rerender(<App connection={{ ...c, busy: true }} />);
  fireEvent.click(screen.getByRole("button", { name: "Results" }));
  fireEvent.click(screen.getByRole("button", { name: `Edit ${beta.name}` }));
  fireEvent.click(screen.getByRole("button", { name: "Sign out" }));
  expect(c.signOut).not.toHaveBeenCalled();
  expect(
    screen.getByRole("heading", { name: "Roster", level: 1 }),
  ).toBeTruthy();
  expect(
    (
      screen.getByRole("textbox", {
        name: "Main name",
      }) as HTMLInputElement
    ).value,
  ).toBe(alpha.name);
  const unloading = new Event("beforeunload", { cancelable: true });
  window.dispatchEvent(unloading);
  expect(unloading.defaultPrevented).toBe(true);
  rerender(<App connection={c} />);
  fireEvent.click(screen.getByRole("button", { name: "Results" }));
  expect(
    screen.getByRole("heading", { name: "Results", level: 1 }),
  ).toBeTruthy();
});

it("walks through four numbered steps and carries the reviewed night to results", async () => {
  const c = dates(
    connection([alpha]),
    ["2026-10-15", "2026-10-14"],
    ["2026-10-15"],
  );
  c.hasResults = true;
  c.saved.results.nights = ["2026-10-15", "2026-10-14"].map((night) => ({
    night,
    duration: 180,
    reports: 1,
    timing: { start: "", end: "", breakStart: "", breakEnd: "", notes: "" },
    players: [],
  }));
  const { rerender } = render(<App connection={c} />);
  const flow = screen.getByRole("navigation", { name: "Process a raid night" });
  expect(within(flow).getAllByRole("listitem")).toHaveLength(4);
  expect(screen.queryByText(/Selected raid night:/)).toBeNull();
  await act(async () =>
    fireEvent.click(
      screen.getByRole("button", {
        name: "Next: Run Pebble and go to Review Night",
      }),
    ),
  );
  expect(screen.getByRole("heading", { level: 1 }).textContent).toBe("Reports");
  rerender(
    <App
      connection={{
        ...c,
        saved: {
          ...c.saved,
          results: { ...c.saved.results, completedAt: "2026-10-23T06:00:00Z" },
        },
      }}
    />,
  );
  await waitFor(() =>
    expect(screen.getByRole("heading", { level: 1 }).textContent).toBe(
      "Review Night",
    ),
  );
  fireEvent.change(raidPicker(), { target: { value: "2026-10-14" } });
  fireEvent.click(screen.getByRole("button", { name: "Update roster →" }));
  expect(
    screen.getByRole("navigation", { name: "Main navigation" }),
  ).toBeTruthy();
  fireEvent.click(
    screen.getByRole("button", { name: "← Return to night review" }),
  );
  expect(raidPicker().value).toBe("2026-10-14");
  fireEvent.click(
    screen.getByRole("button", { name: "Next: Review overrides →" }),
  );
  expect(raidPicker().value).toBe("2026-10-14");
  fireEvent.click(screen.getByRole("button", { name: "Next: Results →" }));
  expect(
    (
      screen.getByRole("combobox", {
        name: "Result period",
      }) as HTMLSelectElement
    ).value,
  ).toBe("2026-10-14");
  expect(screen.queryByRole("status", { name: "Night completion" })).toBeNull();
  expect(c.requestRun).toHaveBeenCalledOnce();
});

it("recalculates saved changes before advancing and stays on review after a failure", async () => {
  const c = dates(connection([alpha]), ["2026-10-15"]);
  c.hasResults = true;
  c.unprocessed = true;
  const { rerender } = render(<App connection={c} />);
  fireEvent.click(screen.getByRole("button", { name: "Review Night" }));
  await act(async () =>
    fireEvent.click(
      screen.getByRole("button", {
        name: "Recalculate and review overrides →",
      }),
    ),
  );
  expect(c.requestRun).toHaveBeenCalledOnce();
  expect(screen.getByRole("heading", { level: 1 }).textContent).toBe(
    "Review Night",
  );
  rerender(
    <App
      connection={{ ...c, active: { id: "new", status: "running", stage: 1 } }}
    />,
  );
  rerender(
    <App
      connection={{
        ...c,
        saved: {
          ...c.saved,
          history: [
            {
              id: "new",
              status: "failed",
              at: new Date().toISOString(),
              message: "Failure fixture",
            },
          ],
        },
      }}
    />,
  );
  expect(screen.getByRole("heading", { level: 1 }).textContent).toBe(
    "Review Night",
  );
  expect(
    screen.getByText("The run failed. Review the run history and try again."),
  ).toBeTruthy();
  await act(async () =>
    fireEvent.click(
      screen.getByRole("button", {
        name: "Recalculate and review overrides →",
      }),
    ),
  );
  const finished = {
    ...c,
    unprocessed: false,
    saved: {
      ...c.saved,
      results: { ...c.saved.results, completedAt: "2026-10-23T05:00:00Z" },
    },
  };
  rerender(<App connection={{ ...finished, resultMetadataPending: true }} />);
  expect(screen.getByRole("heading", { level: 1 }).textContent).toBe(
    "Review Night",
  );
  rerender(<App connection={finished} />);
  await waitFor(() =>
    expect(screen.getByRole("heading", { level: 1 }).textContent).toBe(
      "Overrides",
    ),
  );
});

it("stays on Reports through a queued run and result loading, then reviews the newly added report's night", async () => {
  const c = dates(connection([alpha]), ["2026-09-30"], [""]);
  c.hasResults = true;
  c.unprocessed = true;
  const { rerender } = render(<App connection={c} />);
  expect(screen.queryByText(/Selected raid night:/)).toBeNull();
  await act(async () =>
    fireEvent.click(
      screen.getByRole("button", {
        name: "Next: Run Pebble and go to Review Night",
      }),
    ),
  );
  rerender(
    <App
      connection={{
        ...c,
        online: false,
        active: { id: "new", status: "queued", stage: 0 },
      }}
    />,
  );
  expect(
    screen.getByRole("status", { name: "Run status" }).textContent,
  ).toContain("waiting for your local bot");
  expect(
    (screen.getByRole("button", { name: "Run queued…" }) as HTMLButtonElement)
      .disabled,
  ).toBe(true);
  const finished = dates(
    c,
    ["2026-10-22", "2026-10-21", "2026-09-30"],
    ["2026-10-21"],
  );
  finished.unprocessed = false;
  finished.saved.results = {
    ...finished.saved.results,
    completedAt: "2026-10-23T06:30:00Z",
  };
  rerender(<App connection={{ ...finished, resultsPending: true }} />);
  expect(screen.getByRole("heading", { level: 1 }).textContent).toBe("Reports");
  rerender(<App connection={{ ...finished, resultMetadataPending: true }} />);
  expect(screen.getByRole("heading", { level: 1 }).textContent).toBe("Reports");
  rerender(<App connection={finished} />);
  await waitFor(() => expect(raidPicker().value).toBe("2026-10-21"));
  expect(c.requestRun).toHaveBeenCalledOnce();
});

it("keeps Reports available for retry after submission rejection or worker failure", async () => {
  const c = dates(connection([alpha]), ["2026-09-30"], [""]);
  c.requestRun = vi
    .fn()
    .mockRejectedValueOnce(new Error("Submission unavailable"))
    .mockResolvedValue(undefined);
  const { rerender } = render(<App connection={c} />);
  const next = () =>
    screen.getByRole("button", {
      name: "Next: Run Pebble and go to Review Night",
    });
  await act(async () => fireEvent.click(next()));
  expect(screen.getByText("Submission unavailable")).toBeTruthy();
  expect((next() as HTMLButtonElement).disabled).toBe(false);
  await act(async () => fireEvent.click(next()));
  rerender(
    <App
      connection={{
        ...c,
        saved: {
          ...c.saved,
          history: [
            {
              id: "failed",
              status: "failed",
              at: "2026-10-23T06:30:00Z",
              message: "Worker cannot access report",
            },
          ],
        },
      }}
    />,
  );
  expect(screen.getByRole("heading", { level: 1 }).textContent).toBe("Reports");
  expect(
    screen.getByText("The run failed. Review the run history and try again."),
  ).toBeTruthy();
  expect((next() as HTMLButtonElement).disabled).toBe(false);
});

it.each(["main", "alt", "ignore"] as const)(
  "resolves a missing player inline as %s",
  async (mode) => {
    const c = dates(connection([alpha]), ["2026-10-21"]);
    c.hasResults = true;
    c.roster = [
      { ...blankPlayer(), id: alpha.id, name: alpha.name, version: 1 },
    ];
    c.resultDiagnostics = {
      "2026-10-21": {
        reportCount: 1,
        mythicFights: 2,
        unmappedCharacters: 1,
        breakMinutes: 10,
        timing: { start: "detected", end: "detected", break: "detected" },
        missingPlayers: [
          {
            name: "Extra",
            realm: "Azjol-Nerub",
            region: "eu",
            className: "DeathKnight",
          },
        ],
      },
    };
    c.saved.results.nights = [
      {
        night: "2026-10-21",
        duration: 170,
        reports: 1,
        players: [],
        timing: {
          start: "2026-10-21T19:00",
          end: "2026-10-21T22:00",
          breakStart: "2026-10-21T20:30",
          breakEnd: "2026-10-21T20:40",
          notes: "",
        },
      },
    ];
    render(<App connection={c} />);
    fireEvent.click(screen.getByRole("button", { name: "Review Night" }));
    expect(screen.getByText("7:00 PM")).toBeTruthy();
    expect(screen.getByText("8:30 PM – 8:40 PM (10 min)")).toBeTruthy();
    const row = screen.getByRole("article", { name: "Missing player Extra" });
    await act(async () =>
      fireEvent.click(
        within(row).getByRole("button", {
          name:
            mode === "main"
              ? "Add main"
              : mode === "alt"
                ? "Add alt"
                : "Ignore forever",
        }),
      ),
    );
    if (mode === "main") {
      expect(screen.getByText("Join date: 2026-10-21")).toBeTruthy();
      expect(
        screen.queryByRole("combobox", { name: "Role for Extra" }),
      ).toBeNull();
      await act(async () =>
        fireEvent.click(screen.getByRole("button", { name: "Save main" })),
      );
      expect(c.savePlayer).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          name: "Extra",
          joinDate: "2026-10-21",
          active: true,
          className: "Death Knight",
        }),
        0,
      );
    } else if (mode === "alt") {
      fireEvent.change(
        screen.getByRole("combobox", { name: "Main for Extra" }),
        { target: { value: alpha.id } },
      );
      await act(async () =>
        fireEvent.click(screen.getByRole("button", { name: "Save alt" })),
      );
      expect(c.saveCharacter).toHaveBeenCalledWith(
        expect.any(String),
        {
          name: "Extra",
          realm: "Azjol-Nerub",
          region: "eu",
          playerId: alpha.id,
        },
        0,
      );
    } else {
      expect(c.ignoreCharacter).not.toHaveBeenCalled();
      fireEvent.click(
        screen.getByRole("button", { name: "Cancel player change" }),
      );
      expect(c.ignoreCharacter).not.toHaveBeenCalled();
      fireEvent.click(
        within(row).getByRole("button", { name: "Ignore forever" }),
      );
      await act(async () =>
        fireEvent.click(screen.getByRole("button", { name: "Confirm ignore" })),
      );
      expect(c.ignoreCharacter).toHaveBeenCalledWith(expect.any(String), {
        name: "Extra",
        realm: "Azjol-Nerub",
        region: "eu",
      });
    }
    expect(screen.getByRole("heading", { level: 1 }).textContent).toBe(
      "Review Night",
    );
    expect(c.requestRun).not.toHaveBeenCalled();
  },
);

it("keeps published warning nights in the officer sidebar across panels and guards date navigation", () => {
  const c = connection([alpha]);
  c.hasResults = true;
  c.nights = ["2026-10-14", "2026-10-15"];
  c.saved.results.warnings = [
    "2026-10-14: First",
    "2026-10-15: Latest",
    "2026-10-14: Second",
    "General warning",
  ];
  const { rerender } = render(<App connection={c} />);
  const warningPanel = () =>
    screen.getByRole("region", { name: "Nights to review" });
  expect(
    within(warningPanel())
      .getAllByRole("button")
      .map((button) => button.textContent),
  ).toEqual(["Oct 151 warning", "Oct 142 warnings", "Other warnings1 warning"]);
  fireEvent.click(
    screen.getByRole("button", {
      name: "Review warnings for 2026-10-14: 2 warnings",
    }),
  );
  expect(
    (screen.getByRole("combobox", { name: "Raid night" }) as HTMLSelectElement)
      .value,
  ).toBe("2026-10-14");
  overrides();
  fireEvent.change(notes(), { target: { value: "Retain draft" } });
  fireEvent.click(
    screen.getByRole("button", {
      name: "Review warnings for 2026-10-15: 1 warning",
    }),
  );
  expect(
    (screen.getByRole("combobox", { name: "Raid night" }) as HTMLSelectElement)
      .value,
  ).toBe("2026-10-14");
  fireEvent.click(
    screen.getByRole("button", { name: "Keep editing", hidden: true }),
  );
  expect(notes().value).toBe("Retain draft");
  fireEvent.click(
    screen.getByRole("button", {
      name: "Review warnings for 2026-10-15: 1 warning",
    }),
  );
  fireEvent.click(
    screen.getByRole("button", { name: "Discard edits", hidden: true }),
  );
  expect(
    (screen.getByRole("combobox", { name: "Raid night" }) as HTMLSelectElement)
      .value,
  ).toBe("2026-10-15");
  fireEvent.click(screen.getByRole("button", { name: "Results" }));
  expect(warningPanel()).toBeTruthy();
  expect(screen.queryByRole("status", { name: "Night completion" })).toBeNull();
  rerender(
    <App
      connection={{
        ...c,
        saved: { ...c.saved, results: { ...c.saved.results, warnings: [] } },
      }}
    />,
  );
  expect(screen.queryByRole("region", { name: "Nights to review" })).toBeNull();
  rerender(<App connection={{ ...c, officer: false }} />);
  expect(screen.queryByRole("region", { name: "Nights to review" })).toBeNull();
});

it("reviews a re-added older report even when prior run metadata already marks it fetched", async () => {
  const c = dates(connection([alpha]), ["2026-10-22"], []);
  c.hasResults = true;
  const { rerender } = render(<App connection={c} />);
  fireEvent.change(
    screen.getByRole("textbox", { name: "Warcraft Logs reports" }),
    { target: { value: "AAAAAAAAAAAAAAAA" } },
  );
  await act(async () =>
    fireEvent.click(screen.getByRole("button", { name: "Add report" })),
  );
  const withReport = {
    ...c,
    saved: {
      ...c.saved,
      inputs: {
        ...c.saved.inputs,
        reports: [
          {
            code: "AAAAAAAAAAAAAAAA",
            enabled: true,
            fetched: true,
            title: "Cached report",
            night: "2026-10-14",
          },
        ],
      },
    },
  };
  rerender(<App connection={withReport} />);
  await act(async () =>
    fireEvent.click(
      screen.getByRole("button", {
        name: "Next: Run Pebble and go to Review Night",
      }),
    ),
  );
  rerender(
    <App
      connection={{
        ...withReport,
        saved: {
          ...withReport.saved,
          results: { ...c.saved.results, completedAt: "2026-10-23T08:00:00Z" },
        },
      }}
    />,
  );
  await waitFor(() => expect(raidPicker().value).toBe("2026-10-14"));
  expect(screen.getByRole("heading", { level: 1 }).textContent).toBe(
    "Review Night",
  );
});

it("keeps a missing-player draft cancellable when another officer resolves that player", () => {
  const c = dates(connection([alpha]), ["2026-10-15"]);
  c.hasResults = true;
  c.resultDiagnostics = {
    "2026-10-15": {
      reportCount: 1,
      mythicFights: 1,
      unmappedCharacters: 1,
      breakMinutes: null,
      timing: { start: "detected", end: "detected", break: "none" },
      missingPlayers: [
        { name: "Extra", realm: "Example", region: "eu", className: "Mage" },
      ],
    },
  };
  const { rerender } = render(<App connection={c} />);
  fireEvent.click(screen.getByRole("button", { name: "Review Night" }));
  fireEvent.click(screen.getByRole("button", { name: "Add main" }));
  rerender(
    <App
      connection={{
        ...c,
        roster: [{ ...blankPlayer(), id: "extra", name: "Extra", version: 1 }],
      }}
    />,
  );
  expect(
    screen.getByRole("combobox", { name: "Class for Extra" }),
  ).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "Cancel player change" }));
  expect(screen.getByText(/Main added · joins/)).toBeTruthy();
  expect(
    (
      screen.getByRole("button", {
        name: "Next: Review overrides →",
      }) as HTMLButtonElement
    ).disabled,
  ).toBe(false);
});

it("lists season ignores, filters identities, and undoes with the displayed version", async () => {
  const c = connection();
  c.ignoredCharacters = [
    {
      id: "one",
      name: "Visitor",
      realm: "Example",
      region: "us",
      playerId: "",
      ignored: true,
      version: 3,
    },
    {
      id: "two",
      name: "Guest",
      realm: "Other",
      region: "eu",
      playerId: "",
      ignored: true,
      version: 1,
    },
  ];
  const view = render(<App connection={c} />);
  fireEvent.click(screen.getByRole("button", { name: "Roster" }));
  expect(screen.getByText(/Season: Example season/)).toBeTruthy();
  const panel = screen.getByRole("region", { name: "Ignored players (2)" });
  expect(within(panel).getByText("Visitor")).toBeTruthy();
  fireEvent.change(
    screen.getByRole("textbox", { name: "Search ignored players" }),
    { target: { value: "EXAMPLE" } },
  );
  expect(within(panel).queryByText("Guest")).toBeNull();
  fireEvent.click(
    within(panel).getByRole("button", {
      name: "Undo ignore for Visitor (Example, US)",
    }),
  );
  await waitFor(() =>
    expect(c.undoIgnoreCharacter).toHaveBeenCalledWith("one", 3),
  );
  await waitFor(() =>
    expect(within(panel).getByRole("status").textContent).toContain(
      "Run Pebble",
    ),
  );
  view.rerender(
    <App connection={{ ...c, ignoredCharacters: [c.ignoredCharacters[1]] }} />,
  );
  expect(
    screen.getByRole("region", { name: "Ignored players (1)" }),
  ).toBeTruthy();
  expect(
    screen.queryByRole("button", { name: /Undo ignore for Visitor/ }),
  ).toBeNull();
});

it("keeps failed ignores visible for retry and does not expose them publicly", async () => {
  const c = connection();
  c.ignoredCharacters = [
    {
      id: "one",
      name: "Visitor",
      realm: "Example",
      region: "us",
      playerId: "",
      ignored: true,
      version: 3,
    },
  ];
  vi.mocked(c.undoIgnoreCharacter).mockRejectedValueOnce(
    new Error(
      "This ignore decision changed. Review the latest list and try again.",
    ),
  );
  const view = render(<App connection={c} />);
  fireEvent.click(screen.getByRole("button", { name: "Roster" }));
  fireEvent.click(
    screen.getByRole("button", { name: /Undo ignore for Visitor/ }),
  );
  await waitFor(() =>
    expect(screen.getByRole("alert").textContent).toContain(
      "ignore decision changed",
    ),
  );
  expect(
    screen
      .getByRole("button", { name: /Undo ignore for Visitor/ })
      .hasAttribute("disabled"),
  ).toBe(false);
  view.rerender(
    <App connection={{ ...c, officer: false, accessGeneration: 1 }} />,
  );
  expect(
    screen.queryByRole("textbox", { name: "Search ignored players" }),
  ).toBeNull();
  expect(screen.queryByText("Visitor")).toBeNull();
});

it("guards invitation account switching while an officer has unsaved report edits", async () => {
  const live = connection();
  live.user = { uid: "current", email: "current@example.test" } as NonNullable<
    LiveConnection["user"]
  >;
  vi.mocked(officerAction).mockResolvedValueOnce({
    invitations: [],
    invitation: {
      id: "a".repeat(48),
      email: "intended@example.test",
      invitedByName: "Inviting Officer",
      expiresAt: "2026-10-30T00:00:00Z",
      status: "pending",
      matchesAccount: false,
    },
  });
  history.replaceState(null, "", "/?invite=" + "a".repeat(48));
  render(<App connection={live} />);
  await screen.findByRole("button", { name: "Switch Google account" });
  fireEvent.change(
    screen.getByRole("textbox", { name: "Warcraft Logs reports" }),
    { target: { value: "AAAAAAAAAAAAAAAA" } },
  );
  fireEvent.click(
    screen.getByRole("button", { name: "Switch Google account" }),
  );
  expect(live.signIn).not.toHaveBeenCalled();
  expect(HTMLDialogElement.prototype.showModal).toHaveBeenCalled();
  await act(async () =>
    fireEvent.click(
      screen.getByRole("button", { name: "Discard edits", hidden: true }),
    ),
  );
  expect(live.signIn).toHaveBeenCalledOnce();
});
