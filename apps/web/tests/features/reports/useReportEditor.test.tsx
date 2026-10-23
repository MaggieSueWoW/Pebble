// @vitest-environment jsdom
import { act, cleanup, renderHook } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { useReportEditor } from "../../../src/features/reports/useReportEditor";
import type { Report } from "../../../src/shared/model";
import type { Versions } from "../../../src/data/useFirestore";
const a = "AAAAAAAAAAAAAAAA",
  b = "BBBBBBBBBBBBBBBB",
  c = "CCCCCCCCCCCCCCCC";
const report = (code: string, enabled = true): Report => ({
  code,
  enabled,
  fetched: false,
  title: "",
  night: "",
});
afterEach(cleanup);
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((yes) => {
    resolve = yes;
  });
  return { promise, resolve };
}
it("coalesces repeated saves and preserves text entered during a pending add", async () => {
  const wait = deferred(),
    save = vi.fn((_changes: Report[], _expected: Versions) => wait.promise);
  const { result } = renderHook(() => useReportEditor([], {}, save, vi.fn()));
  act(() => result.current.setText(a));
  let done!: Promise<void>;
  act(() => {
    done = result.current.add();
    void result.current.add();
    result.current.reset();
  });
  expect(save).toHaveBeenCalledTimes(1);
  expect(result.current.saving).toBe(true);
  act(() => result.current.setText(c));
  await act(async () => {
    wait.resolve();
    await done;
  });
  expect(result.current.text).toBe(c);
  expect(result.current.dirty).toBe(true);
});
it("keeps failed adds for retry and clears only acknowledged drafts", async () => {
  const save = vi
    .fn()
    .mockRejectedValueOnce(new Error("Save unavailable"))
    .mockResolvedValue(undefined);
  const { result } = renderHook(() => useReportEditor([], {}, save, vi.fn()));
  act(() => result.current.setText(a));
  await act(() => result.current.add());
  expect(result.current.error).toBe("Save unavailable");
  expect(result.current.text).toBe(a);
  await act(() => result.current.add());
  expect(result.current.text).toBe("");
  expect(result.current.error).toBe("");
});
it("rejects invalid batches and deduplicates URLs and codes", async () => {
  const save = vi.fn().mockResolvedValue(undefined);
  const { result } = renderHook(() => useReportEditor([], {}, save, vi.fn()));
  act(() => result.current.setText(`${a}\nbad`));
  await act(() => result.current.add());
  expect(save).not.toHaveBeenCalled();
  act(() =>
    result.current.setText(`${a}\nhttps://www.warcraftlogs.com/reports/${a}`),
  );
  await act(() => result.current.add());
  expect(save.mock.calls[0][0]).toHaveLength(1);
  expect(save.mock.calls[0][1]).toEqual({ [`reports/${a}`]: 0 });
});
it("freezes each edited row's version and retains conflicting selections", async () => {
  const save = vi.fn().mockRejectedValue(new Error("Conflict: cancel edits"));
  const rows = [report(a), report(b)];
  const { result, rerender } = renderHook(
    ({ versions }: { versions: Versions }) =>
      useReportEditor(rows, versions, save, vi.fn()),
    {
      initialProps: { versions: { [`reports/${a}`]: 1, [`reports/${b}`]: 1 } },
    },
  );
  act(() => result.current.toggle(a));
  rerender({ versions: { [`reports/${a}`]: 2, [`reports/${b}`]: 3 } });
  act(() => result.current.toggle(b));
  await act(() => result.current.saveSelection());
  expect(save.mock.calls[0][1]).toEqual({
    [`reports/${a}`]: 1,
    [`reports/${b}`]: 3,
  });
  expect(result.current.edits).toEqual({ [a]: false, [b]: false });
  expect(result.current.error).toContain("Conflict");
  act(() => result.current.cancelSelection());
  expect(result.current.dirty).toBe(false);
  act(() => result.current.toggle(a));
  await act(() => result.current.saveSelection());
  expect(save.mock.calls[1][1]).toEqual({ [`reports/${a}`]: 2 });
});
it("locks selection controls during saves and writes only edited rows", async () => {
  const wait = deferred(),
    save = vi.fn((_changes: Report[], _expected: Versions) => wait.promise);
  const { result } = renderHook(() =>
    useReportEditor([report(a), report(b)], {}, save, vi.fn()),
  );
  act(() => result.current.toggle(a));
  let done!: Promise<void>;
  act(() => {
    done = result.current.saveSelection();
    result.current.toggle(b);
    result.current.cancelSelection();
    void result.current.saveSelection();
  });
  expect(result.current.edits).toEqual({ [a]: false });
  expect(save).toHaveBeenCalledTimes(1);
  expect(save.mock.calls[0][0]).toEqual([report(a, false)]);
  await act(async () => {
    wait.resolve();
    await done;
  });
  expect(result.current.edits).toEqual({});
});
it("re-enables one report, preserving newer text and unrelated selections", async () => {
  const wait = deferred(),
    save = vi.fn((_changes: Report[], _expected: Versions) => wait.promise);
  const { result } = renderHook(() =>
    useReportEditor(
      [report(a, false), report(b)],
      { [`reports/${a}`]: 4 },
      save,
      vi.fn(),
    ),
  );
  act(() => {
    result.current.setText(a);
    result.current.toggle(b);
  });
  let done!: Promise<void>;
  act(() => {
    done = result.current.reenable(a);
    void result.current.reenable(a);
  });
  act(() => result.current.setText(c));
  await act(async () => {
    wait.resolve();
    await done;
  });
  expect(save).toHaveBeenCalledTimes(1);
  expect(save.mock.calls[0]).toEqual([[report(a)], { [`reports/${a}`]: 4 }]);
  expect(result.current.text).toBe(c);
  expect(result.current.edits).toEqual({ [b]: false });
});
it("blocks writes while another input save is busy", async () => {
  const save = vi.fn();
  const { result } = renderHook(() =>
    useReportEditor([report(a, false)], {}, save, vi.fn(), true),
  );
  act(() => {
    result.current.setText(b);
    result.current.toggle(a);
  });
  await act(async () => {
    await result.current.add();
    await result.current.reenable(a);
  });
  expect(save).not.toHaveBeenCalled();
  expect(result.current.edits).toEqual({});
});

it("deletes once with the current version and clears only that report's selection draft", async () => {
  const wait = deferred();
  const remove = vi.fn(() => wait.promise);
  const save = vi.fn();
  const { result } = renderHook(() =>
    useReportEditor(
      [report(a), report(b)],
      { [`reports/${a}`]: 4 },
      save,
      remove,
    ),
  );
  act(() => {
    result.current.toggle(a);
    result.current.toggle(b);
    result.current.setText(c);
  });
  let done!: Promise<void>;
  act(() => {
    done = result.current.remove(a);
    void result.current.remove(a);
  });
  expect(remove).toHaveBeenCalledExactlyOnceWith(a, 4);
  expect(result.current.saving).toBe(true);
  await act(async () => {
    wait.resolve();
    await done;
  });
  expect(result.current.edits).toEqual({ [b]: false });
  expect(result.current.text).toBe(c);
  expect(result.current.message).toContain("Report deleted");
  expect(save).not.toHaveBeenCalled();
});
it("keeps selection drafts when deletion fails and allows retry", async () => {
  const remove = vi
    .fn()
    .mockRejectedValueOnce(new Error("Another officer changed this report"))
    .mockResolvedValue(undefined);
  const { result } = renderHook(() =>
    useReportEditor([report(a)], {}, vi.fn(), remove),
  );
  act(() => result.current.toggle(a));
  await act(() => result.current.remove(a));
  expect(result.current.error).toContain("Another officer");
  expect(result.current.edits).toEqual({ [a]: false });
  await act(() => result.current.remove(a));
  expect(result.current.error).toBe("");
  expect(result.current.edits).toEqual({});
});
