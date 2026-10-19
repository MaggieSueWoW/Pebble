import { timingSpans, type Timing } from "./model";
import { blankTiming, validateTiming } from "./timing";
import { useVersionedEditor } from "./useVersionedEditor";
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
  demo = false,
) {
  return useVersionedEditor({
    key: night,
    saved,
    version,
    validate: (value) => {
      const error = validateTiming(value, detected);
      if (error || !demo) return error;
      try {
        timingSpans(night, value);
        return null;
      } catch (cause) {
        return (cause as Error).message;
      }
    },
    saveValue,
    resetValue: blankTiming,
    busy,
    label: "Timing",
  });
}
export type TimingEditorState = ReturnType<typeof useTimingEditor>;
