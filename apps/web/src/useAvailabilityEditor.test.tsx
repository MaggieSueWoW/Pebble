// @vitest-environment jsdom
import { act, cleanup, renderHook } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { emptyAdjustment, type Adjustment } from "./model";
import { useAvailabilityEditor } from "./useAvailabilityEditor";
const key = "2026-10-14/player-a",
  bounds = { pre: 90, post: 80 };
const available = (): Adjustment => ({
  ...emptyAdjustment(),
  pre: { mode: "available" },
});
const note = (notes: string): Adjustment => ({ ...available(), notes });
const save = () =>
  vi
    .fn<(key: string, value: Adjustment, version: number) => Promise<number>>()
    .mockImplementation(async (_key, _value, version) => version + 1);
function deferred() {
  let resolve!: (v: number) => void;
  let reject!: (e: Error) => void;
  const promise = new Promise<number>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}
afterEach(cleanup);
it("refreshes pristine snapshots, freezes the first edit's version, and cancels against latest data", async () => {
  const write = save();
  const { result, rerender } = renderHook(
    ({ saved, version }) =>
      useAvailabilityEditor(key, saved, version, bounds, write),
    { initialProps: { saved: emptyAdjustment(), version: 1 } },
  );
  rerender({ saved: note("Remote before editing"), version: 3 });
  expect(result.current.value.notes).toBe("Remote before editing");
  expect(result.current.dirty).toBe(false);
  act(() => result.current.change((old) => ({ ...old, notes: "Draft" })));
  rerender({ saved: note("Remote while editing"), version: 4 });
  expect(result.current.value.notes).toBe("Draft");
  write.mockRejectedValueOnce(new Error("Version conflict"));
  await act(() => result.current.save());
  expect(write).toHaveBeenCalledWith(key, note("Draft"), 3);
  expect(result.current.error).toBe("Version conflict");
  expect(result.current.dirty).toBe(true);
  act(() => result.current.cancel());
  expect(result.current.value.notes).toBe("Remote while editing");
  act(() => result.current.change(note("Retry")));
  await act(() => result.current.save());
  expect(write).toHaveBeenLastCalledWith(key, note("Retry"), 4);
});
it("coalesces saves, locks cancel/reset, preserves newer edits, and uses the acknowledged version", async () => {
  const wait = deferred(),
    write = save().mockReturnValueOnce(wait.promise);
  const { result } = renderHook(() =>
    useAvailabilityEditor(key, emptyAdjustment(), 1, bounds, write),
  );
  act(() => result.current.change(note("Submitted")));
  let done!: Promise<void>;
  act(() => {
    done = result.current.save();
    void result.current.save();
    result.current.cancel();
    result.current.reset();
  });
  expect(write).toHaveBeenCalledTimes(1);
  expect(result.current.value.notes).toBe("Submitted");
  act(() => result.current.change(note("Newer")));
  await act(async () => {
    wait.resolve(7);
    await done;
  });
  expect(result.current.value.notes).toBe("Newer");
  expect(result.current.dirty).toBe(true);
  await act(() => result.current.save());
  expect(write).toHaveBeenLastCalledWith(key, note("Newer"), 7);
});
it("keeps acknowledged values until listener catch-up, then accepts later remote updates", async () => {
  const write = save();
  const { result, rerender } = renderHook(
    ({ saved, version }) =>
      useAvailabilityEditor(key, saved, version, bounds, write),
    { initialProps: { saved: emptyAdjustment(), version: 1 } },
  );
  act(() => result.current.change(available()));
  await act(() => result.current.save());
  expect(result.current.value).toEqual(available());
  expect(result.current.dirty).toBe(false);
  rerender({ saved: available(), version: 2 });
  rerender({ saved: note("Later remote edit"), version: 3 });
  expect(result.current.value.notes).toBe("Later remote edit");
  expect(result.current.dirty).toBe(false);
});
it("preserves a revert entered during a pending save as a new draft", async () => {
  const wait = deferred(),
    write = save().mockReturnValueOnce(wait.promise);
  const { result } = renderHook(() =>
    useAvailabilityEditor(key, emptyAdjustment(), 1, bounds, write),
  );
  act(() => result.current.change(available()));
  let done!: Promise<void>;
  act(() => {
    done = result.current.save();
    result.current.change(emptyAdjustment());
  });
  await act(async () => {
    wait.resolve(2);
    await done;
  });
  expect(result.current.value).toEqual(emptyAdjustment());
  expect(result.current.dirty).toBe(true);
});
it("retains failed saves for retry and makes reset an unsaved change", async () => {
  const write = save().mockRejectedValueOnce(new Error("Denied"));
  const { result } = renderHook(() =>
    useAvailabilityEditor(key, available(), 1, bounds, write),
  );
  act(() => result.current.reset());
  expect(result.current.dirty).toBe(true);
  await act(() => result.current.save());
  expect(result.current.error).toBe("Denied");
  expect(result.current.value).toEqual(emptyAdjustment());
  await act(() => result.current.save());
  expect(result.current.dirty).toBe(false);
  expect(result.current.error).toBe("");
});
it("rejects invalid minutes, busy saves, and missing players", async () => {
  const write = save();
  const { result, rerender } = renderHook(
    ({ busy, enabled }) =>
      useAvailabilityEditor(
        key,
        emptyAdjustment(),
        0,
        bounds,
        write,
        busy,
        enabled,
      ),
    { initialProps: { busy: false, enabled: true } },
  );
  for (const minutes of [undefined, 0, 1.5, 91]) {
    act(() =>
      result.current.change({
        ...emptyAdjustment(),
        pre: { mode: "partial", minutes },
      }),
    );
    await act(() => result.current.save());
    expect(result.current.validation).toContain("Before break");
  }
  act(() => result.current.change(available()));
  rerender({ busy: true, enabled: true });
  await act(() => result.current.save());
  rerender({ busy: false, enabled: false });
  await act(() => result.current.save());
  expect(write).not.toHaveBeenCalled();
});
it("ignores acknowledgements from an abandoned context, even after returning to its key", async () => {
  const wait = deferred(),
    write = save().mockReturnValueOnce(wait.promise);
  const { result, rerender } = renderHook(
    ({ contextKey, saved }) =>
      useAvailabilityEditor(contextKey, saved, 1, bounds, write),
    { initialProps: { contextKey: key, saved: emptyAdjustment() } },
  );
  act(() => result.current.change(note("Old draft")));
  let done!: Promise<void>;
  act(() => {
    done = result.current.save();
  });
  rerender({ contextKey: "2026-10-15/player-b", saved: note("Other player") });
  expect(result.current.value.notes).toBe("Other player");
  rerender({ contextKey: key, saved: note("Fresh snapshot") });
  await act(async () => {
    wait.resolve(2);
    await done;
  });
  expect(result.current.value.notes).toBe("Fresh snapshot");
  expect(result.current.message).toBe("");
  expect(result.current.dirty).toBe(false);
});
it("does not apply a late failure after unmount", async () => {
  const wait = deferred(),
    write = save().mockReturnValueOnce(wait.promise);
  const { result, unmount } = renderHook(() =>
    useAvailabilityEditor(key, emptyAdjustment(), 0, bounds, write),
  );
  act(() => result.current.change(available()));
  let done!: Promise<void>;
  act(() => {
    done = result.current.save();
  });
  unmount();
  await act(async () => {
    wait.reject(new Error("Late failure"));
    await done;
  });
  expect(write).toHaveBeenCalledTimes(1);
});
