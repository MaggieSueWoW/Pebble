import type { Timing } from "./model";
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
  validateSample?: (night: string, value: Timing) => void,
) {
  return useVersionedEditor({
    key: night,
    saved,
    version,
    validate: (value) => {
      const error = validateTiming(value, detected);
      if (error || !validateSample) return error;
      try {
        validateSample(night, value);
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
