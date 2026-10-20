// @vitest-environment jsdom
import { act, cleanup, renderHook } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { defaultCalculation } from "./calculation";
import { useCalculationEditor } from "./useCalculationEditor";
afterEach(cleanup);
it("freezes calculation versions, retains conflicts and cancels to latest settings", async () => {
  const write = vi.fn().mockRejectedValueOnce(new Error("Conflict"));
  const { result, rerender } = renderHook(
    ({ saved, version }) => useCalculationEditor(saved, version, write),
    { initialProps: { saved: defaultCalculation(), version: 2 } },
  );
  act(() =>
    result.current.change((old) => ({ ...old, postExtensionMinutes: 8 })),
  );
  const remote = { ...defaultCalculation(), postExtensionMinutes: 10 };
  rerender({ saved: remote, version: 3 });
  await act(() => result.current.save());
  expect(write).toHaveBeenCalledWith(
    { ...defaultCalculation(), postExtensionMinutes: 8 },
    2,
  );
  expect(result.current.error).toBe("Conflict");
  act(() => result.current.cancel());
  expect(result.current.value).toEqual(remote);
});
it("blocks invalid drafts, repeated pending saves and unauthorized editing", async () => {
  let resolve!: (version: number) => void;
  const write = vi.fn(
    () =>
      new Promise<number>((yes) => {
        resolve = yes;
      }),
  );
  const { result, rerender } = renderHook(
    ({ enabled }) =>
      useCalculationEditor(defaultCalculation(), 0, write, false, enabled),
    { initialProps: { enabled: true } },
  );
  act(() => result.current.change((old) => ({ ...old, minBreakMinutes: 0 })));
  await act(() => result.current.save());
  expect(write).not.toHaveBeenCalled();
  act(() => result.current.change((old) => ({ ...old, minBreakMinutes: 12 })));
  let done!: Promise<void>;
  act(() => {
    done = result.current.save();
    void result.current.save();
    result.current.cancel();
  });
  expect(write).toHaveBeenCalledTimes(1);
  await act(async () => {
    resolve(1);
    await done;
  });
  expect(result.current.dirty).toBe(false);
  expect(result.current.message).toContain("Run Pebble");
  rerender({ enabled: false });
  act(() => result.current.change((old) => ({ ...old, minBreakMinutes: 13 })));
  expect(result.current.dirty).toBe(false);
});
