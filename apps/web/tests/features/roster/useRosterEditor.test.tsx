// @vitest-environment jsdom
import { act, renderHook, cleanup, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { useRosterEditor } from "../../../src/features/roster/useRosterEditor";
import {
  blankPlayer,
  type RosterEntry,
} from "../../../src/features/roster/roster";
afterEach(cleanup);
const initial: RosterEntry = {
  ...blankPlayer(),
  id: "stable",
  version: 3,
  name: "Example",
  notes: "Keep notes",
};
it("keeps edits bound to the original version, preserves notes, and cancels to current remote values", async () => {
  const save = vi
    .fn()
    .mockRejectedValue(new Error("Another officer changed this player"));
  const { result, rerender } = renderHook(
    ({ roster }) => useRosterEditor(roster, save, false, true),
    { initialProps: { roster: [initial] } },
  );
  act(() => result.current.change((old) => ({ ...old, name: "Renamed" })));
  rerender({ roster: [{ ...initial, version: 4, name: "Remote" }] });
  await act(() => result.current.save());
  expect(save).toHaveBeenCalledWith(
    "stable",
    { ...blankPlayer(), name: "Renamed", notes: "Keep notes" },
    3,
  );
  expect(result.current.value.name).toBe("Renamed");
  expect(result.current.error).toContain("Another officer");
  act(() => result.current.cancel());
  expect(result.current.value.name).toBe("Remote");
});
it("adds a generated stable ID, acknowledges a save and retains inactive records", async () => {
  const save = vi.fn().mockResolvedValue(1);
  const { result } = renderHook(() =>
    useRosterEditor([initial], save, false, true),
  );
  act(() => result.current.add());
  const id = result.current.id;
  expect(id).not.toBe("stable");
  act(() =>
    result.current.change((old) => ({ ...old, name: "Étoile", active: false })),
  );
  await act(() => result.current.save());
  expect(save).toHaveBeenCalledWith(
    id,
    { ...blankPlayer(), name: "Étoile", active: false },
    0,
  );
  await waitFor(() => expect(result.current.dirty).toBe(false));
  expect(result.current.value.name).toBe("Étoile");
});

it("keeps the selected stable ID when a rename reorders the roster during a held save", async () => {
  let release!: (version: number) => void;
  const save = vi.fn(
    () =>
      new Promise<number>((resolve) => {
        release = resolve;
      }),
  );
  const other = { ...initial, id: "other", name: "Other" };
  const { result, rerender } = renderHook(
    ({ roster }) => useRosterEditor(roster, save, false, true),
    { initialProps: { roster: [initial, other] } },
  );
  act(() => result.current.change((old) => ({ ...old, name: "Zed" })));
  let pending!: Promise<void>;
  act(() => {
    pending = result.current.save();
  });
  rerender({ roster: [other, { ...initial, version: 4, name: "Zed" }] });
  expect(result.current.id).toBe("stable");
  expect(result.current.value.name).toBe("Zed");
  await act(async () => {
    release(4);
    await pending;
  });
  expect(result.current.id).toBe("stable");
  act(() => result.current.change((old) => ({ ...old, notes: "New draft" })));
  rerender({
    roster: [
      { ...other, name: "Another" },
      { ...initial, version: 4, name: "Zed" },
    ],
  });
  expect(result.current.id).toBe("stable");
  expect(result.current.value.notes).toBe("New draft");
});
