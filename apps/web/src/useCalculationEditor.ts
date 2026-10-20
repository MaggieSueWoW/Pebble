import {
  defaultCalculation,
  validateCalculation,
  type CalculationSettings,
} from "./calculation";
import { useVersionedEditor } from "./useVersionedEditor";
export function useCalculationEditor(
  saved: CalculationSettings,
  version: number,
  save: (value: CalculationSettings, expected: number) => Promise<number>,
  busy = false,
  enabled = true,
) {
  return useVersionedEditor({
    key: "calculation",
    saved,
    version,
    validate: validateCalculation,
    saveValue: (_key, value, expected) => save(value, expected),
    resetValue: defaultCalculation,
    busy,
    enabled,
    label: "Calculation settings",
  });
}
export type CalculationEditorState = ReturnType<typeof useCalculationEditor>;
