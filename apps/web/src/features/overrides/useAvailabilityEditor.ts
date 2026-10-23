import {
  emptyAdjustment,
  validateAdjustment,
  type Adjustment,
} from "../../shared/model";
import { useVersionedEditor } from "../../shared/useVersionedEditor";
export function useAvailabilityEditor(
  key: string,
  saved: Adjustment,
  version: number,
  bounds: { pre: number; post: number },
  saveValue: (
    key: string,
    value: Adjustment,
    expected: number,
  ) => Promise<number>,
  busy = false,
  enabled = true,
) {
  return useVersionedEditor({
    key,
    saved,
    version,
    validate: (value) => validateAdjustment(value, bounds.pre, bounds.post),
    saveValue,
    resetValue: emptyAdjustment,
    busy,
    enabled,
    label: "Availability",
  });
}
export type AvailabilityEditorState = ReturnType<typeof useAvailabilityEditor>;
