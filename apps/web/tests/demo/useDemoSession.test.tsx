// @vitest-environment jsdom
import { act, cleanup, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { useDemoSession } from "../../src/demo/useDemoSession";

beforeEach(() => {
  localStorage.clear();
  vi.useFakeTimers();
});
afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.restoreAllMocks();
});

it("keeps offline work queued and publishes the captured inputs without losing later edits", async () => {
  const { result } = renderHook(useDemoSession);
  act(() => result.current.setOnline(false));
  await act(() => result.current.requestRun());
  act(() => vi.advanceTimersByTime(6000));
  expect(result.current.active?.status).toBe("queued");

  act(() => result.current.setOnline(true));
  act(() => vi.advanceTimersByTime(650));
  expect(result.current.active?.status).toBe("running");
  const capturedCount = result.current.saved.inputs.reports.filter(
    (r) => r.enabled,
  ).length;
  const report = result.current.saved.inputs.reports[0];
  await act(() =>
    result.current.saveInputs(
      {
        reports: [{ ...report, enabled: false }],
        adjustments: {},
        timings: {},
      },
      { [`reports/${report.code}`]: 0 },
    ),
  );
  act(() => vi.advanceTimersByTime(4800));

  expect(result.current.active).toBeNull();
  expect(result.current.saved.results.reportCount).toBe(capturedCount);
  expect(result.current.saved.inputs.reports[0].enabled).toBe(false);
  expect(result.current.unprocessed).toBe(true);
  expect(result.current.saved.history[0].status).toBe("succeeded");
  expect(result.current.notification?.error).toBe(false);
});

it("retains completed results on simulated failure and consumes fail-next once", async () => {
  const { result } = renderHook(useDemoSession);
  const previous = result.current.saved.results;
  act(() => result.current.setFailNext(true));
  await act(() => result.current.requestRun());
  act(() => vi.advanceTimersByTime(650));
  expect(result.current.failNext).toBe(false);
  act(() => vi.advanceTimersByTime(4800));
  expect(result.current.saved.results).toBe(previous);
  expect(result.current.saved.history[0].status).toBe("failed");
  expect(result.current.notification?.error).toBe(true);

  await act(() => result.current.requestRun());
  act(() => vi.advanceTimersByTime(650));
  act(() => vi.advanceTimersByTime(4800));
  expect(result.current.saved.history[0].status).toBe("succeeded");
});

it("persists sample edits across remounts and cancels simulation timers on unmount", async () => {
  const first = renderHook(useDemoSession);
  const report = first.result.current.saved.inputs.reports[0];
  await act(() =>
    first.result.current.saveInputs({
      reports: [{ ...report, enabled: false }],
      adjustments: {},
      timings: {},
    }),
  );
  await act(() => first.result.current.requestRun());
  act(() => vi.advanceTimersByTime(650));
  first.unmount();
  expect(vi.getTimerCount()).toBe(0);

  const next = renderHook(useDemoSession);
  expect(next.result.current.saved.inputs.reports[0].enabled).toBe(false);
  expect(next.result.current.active).toBeNull();
  act(() => next.result.current.reset());
  expect(next.result.current.saved.inputs.reports[0].enabled).toBe(true);
  expect(next.result.current.saved.history).toHaveLength(1);
});

it("keeps sample editing usable when browser storage is blocked", async () => {
  vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
    throw new Error("Storage blocked");
  });
  const { result } = renderHook(useDemoSession);
  expect(result.current.storageError).toBe(true);
  const report = result.current.saved.inputs.reports[0];
  await act(() =>
    result.current.saveInputs({
      reports: [{ ...report, enabled: false }],
      adjustments: {},
      timings: {},
    }),
  );
  expect(result.current.saved.inputs.reports[0].enabled).toBe(false);
});
