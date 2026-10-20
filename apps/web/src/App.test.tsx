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
    roster: [],
    savePlayer: vi.fn(),
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
    resultSources: [],
    resultsPending: false,
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
    fireEvent.click(
      screen.getByRole("button", { name: "Sign out to switch accounts" }),
    ),
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
    fireEvent.click(
      screen.getByRole("button", { name: "Sign out to switch accounts" }),
    ),
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
    expect(
      Boolean(screen.queryByRole("button", { name: "Manage reports" })),
    ).toBe(officer);
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
          ? "Use Manage reports to review the included logs."
          : "No player time was recorded for this period in the completed results.",
      ),
    ).toBeTruthy();
    if (officer) {
      fireEvent.click(screen.getByRole("button", { name: "Manage reports" }));
      expect(
        screen.getByRole("heading", { name: "Add a report" }),
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

it("labels the visible override panel from its selected tab and links both controls", () => {
  render(<App connection={connection([alpha])} />);
  overrides();
  const availability = screen.getByRole("tab", { name: "Player availability" });
  const timing = screen.getByRole("tab", { name: "Raid timing" });
  expect(screen.getByRole("tabpanel", { name: "Player availability" }).id).toBe(
    availability.getAttribute("aria-controls"),
  );
  fireEvent.click(timing);
  expect(screen.getByRole("tabpanel", { name: "Raid timing" }).id).toBe(
    timing.getAttribute("aria-controls"),
  );
  expect(
    screen.queryByRole("tabpanel", { name: "Player availability" }),
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
      { code: "AAAAAAAAAAAAAAAA", title: "Example source report" },
    ];
    const { rerender } = render(<App connection={c} />);
    if (officer)
      fireEvent.click(screen.getByRole("button", { name: /^Results/ }));
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
        screen.getByRole("region", { name: "Source reports for this run" }),
      ).toBeTruthy();
    } else
      expect(
        screen.queryByRole("link", { name: "Example source report" }),
      ).toBeNull();
    rerender(<App connection={{ ...c, resultSources: [] }} />);
    expect(
      screen.queryByRole("region", { name: "Source reports for this run" }),
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
  const name = screen.getByRole("button", { name: /Example Alpha.*Mage/ });
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

it("shows stored weekly rows, keeps zero time and omits per-night availability actions", () => {
  const c = connection([alpha]);
  c.hasResults = true;
  c.saved.results.weeks = [
    {
      week: "2026-10-13",
      players: [
        {
          id: alpha.id,
          name: alpha.name,
          played: 123,
          bench: 47,
          preBench: 20,
          postBench: 27,
        },
        {
          id: "historical",
          name: "Historical Player",
          played: 0,
          bench: 0,
          preBench: 0,
          postBench: 0,
        },
      ],
    },
  ];
  const { rerender } = render(<App connection={c} />);
  fireEvent.click(screen.getByRole("button", { name: /^Results/ }));
  const period = screen.getByRole("combobox", { name: "Result period" });
  fireEvent.change(period, { target: { value: "week:2026-10-13" } });
  expect(screen.getByText("2h 03m")).toBeTruthy();
  expect(screen.getByText("0h 47m")).toBeTruthy();
  expect(screen.getByText("Historical Player")).toBeTruthy();
  expect(screen.getByText("No recorded time")).toBeTruthy();
  fireEvent.click(
    screen.getByRole("button", { name: `Details for ${alpha.name}` }),
  );
  expect(screen.getByText("20 min")).toBeTruthy();
  expect(
    screen.queryByRole("button", { name: "Edit availability" }),
  ).toBeNull();
  expect(screen.queryByText("Automatically inferred")).toBeNull();
  c.saved.results = { ...c.saved.results, weeks: [] };
  rerender(<App connection={{ ...c }} />);
  expect((period as HTMLSelectElement).value).toBe("season");
});
