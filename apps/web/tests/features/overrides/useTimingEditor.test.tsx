// @vitest-environment jsdom
import { act, cleanup, renderHook } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { timingSpans } from "../../../src/demo/model";
import { blankTiming } from "../../../src/features/overrides/timing";
import type { Timing } from "../../../src/shared/model";
import { useTimingEditor } from "../../../src/features/overrides/useTimingEditor";
const night = "2026-10-14";
const value = (notes: string): Timing => ({ ...blankTiming(), notes });
const save = () =>
  vi
    .fn<(night: string, value: Timing, version: number) => Promise<number>>()
    .mockImplementation(async (_night, _value, version) => version + 1);
function deferred() {
  let resolve!: (v: number) => void;
  const promise = new Promise<number>((yes) => {
    resolve = yes;
  });
  return { promise, resolve };
}
afterEach(cleanup);
it("freezes versions on edit, retains conflicts and cancels against the latest snapshot", async () => {
  const write = save().mockRejectedValueOnce(new Error("Conflict"));
  const { result, rerender } = renderHook(
    ({ saved, version }) =>
      useTimingEditor(night, saved, version, blankTiming(), write),
    { initialProps: { saved: value("Initial"), version: 4 } },
  );
  rerender({ saved: value("Remote before edit"), version: 5 });
  act(() => result.current.change(value("Draft")));
  rerender({ saved: value("Remote after edit"), version: 6 });
  await act(() => result.current.save());
  expect(write).toHaveBeenCalledWith(night, value("Draft"), 5);
  expect(result.current.error).toBe("Conflict");
  expect(result.current.value.notes).toBe("Draft");
  act(() => result.current.cancel());
  expect(result.current.value.notes).toBe("Remote after edit");
  act(() => result.current.change(value("Retry")));
  await act(() => result.current.save());
  expect(write).toHaveBeenLastCalledWith(night, value("Retry"), 6);
});
it("guards repeated saves and reset/cancel while preserving newer fields and acknowledged versions", async () => {
  const wait = deferred(),
    write = save().mockReturnValueOnce(wait.promise);
  const { result } = renderHook(() =>
    useTimingEditor(night, blankTiming(), 1, blankTiming(), write),
  );
  act(() => result.current.change(value("Submitted")));
  let done!: Promise<void>;
  act(() => {
    done = result.current.save();
    void result.current.save();
    result.current.reset();
    result.current.cancel();
  });
  expect(write).toHaveBeenCalledTimes(1);
  expect(result.current.value.notes).toBe("Submitted");
  act(() =>
    result.current.change((old) => ({
      ...old,
      end: `${night}T22:00`,
      notes: "Newer",
    })),
  );
  await act(async () => {
    wait.resolve(8);
    await done;
  });
  expect(result.current.value.notes).toBe("Newer");
  expect(result.current.dirty).toBe(true);
  await act(() => result.current.save());
  expect(write.mock.calls[1][2]).toBe(8);
});
it("ignores abandoned-night acknowledgements and retains automatic reset as a draft", async () => {
  const wait = deferred(),
    write = save().mockReturnValueOnce(wait.promise);
  const { result, rerender } = renderHook(
    ({ key }) => useTimingEditor(key, value("Saved"), 1, blankTiming(), write),
    { initialProps: { key: night } },
  );
  act(() => result.current.reset());
  expect(result.current.dirty).toBe(true);
  let done!: Promise<void>;
  act(() => {
    done = result.current.save();
  });
  rerender({ key: "2026-10-15" });
  await act(async () => {
    wait.resolve(2);
    await done;
  });
  expect(result.current.value.notes).toBe("Saved");
  expect(result.current.dirty).toBe(false);
  expect(result.current.message).toBe("");
});
it("does not write invalid or busy drafts and keeps demo timing validation", async () => {
  const write = save();
  const { result, rerender } = renderHook(
    ({ busy, demo }) =>
      useTimingEditor(
        night,
        blankTiming(),
        0,
        blankTiming(),
        write,
        busy,
        demo ? timingSpans : undefined,
      ),
    { initialProps: { busy: false, demo: false } },
  );
  act(() =>
    result.current.change({ ...blankTiming(), breakStart: `${night}T20:00` }),
  );
  await act(() => result.current.save());
  expect(result.current.validation).toContain("both break boundaries");
  act(() => result.current.change(value("Valid notes")));
  rerender({ busy: true, demo: false });
  await act(() => result.current.save());
  rerender({ busy: false, demo: true });
  act(() =>
    result.current.change({ ...blankTiming(), start: `${night}T23:00` }),
  );
  await act(() => result.current.save());
  expect(result.current.validation).toContain("Timing must follow");
  expect(write).not.toHaveBeenCalled();
});
