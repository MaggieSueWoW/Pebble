import type { Timing } from "../../shared/model";
import { blankTiming, validateTiming } from "./timing";
import { useVersionedEditor } from "../../shared/useVersionedEditor";
export function useTimingEditor(
  night: string,
  saved: Timing,
  version: number,
  detected: Timing,
  saveValue: (
    night: string,
    value: Timing,
    expected: number,
  ) => Promise<number>,
  busy = false,
) {
  return useVersionedEditor({
    key: night,
    saved,
    version,
    validate: (value) => validateTiming(value, detected),
    saveValue,
    resetValue: blankTiming,
    busy,
    label: "Timing",
  });
}
export type TimingEditorState = ReturnType<typeof useTimingEditor>;
