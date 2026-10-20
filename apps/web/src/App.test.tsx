// @vitest-environment jsdom
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
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
