import { ArrowDown, ArrowUp, ArrowUpDown } from "lucide-react";
import type { ComponentProps } from "react";

/** Shared visual treatment; table headers provide aria-sort for table controls. */
export function SortButton({
  label,
  direction,
  ...props
}: Omit<ComponentProps<"button">, "children"> & {
  label: string;
  direction?: "ascending" | "descending";
}) {
  return (
    <button
      type="button"
      className="result-sort-button"
      title={`Sort by ${label}`}
      {...props}
    >
      {label}
      {direction === "ascending" ? (
        <ArrowUp size={12} aria-hidden="true" />
      ) : direction === "descending" ? (
        <ArrowDown size={12} aria-hidden="true" />
      ) : (
        <ArrowUpDown size={12} aria-hidden="true" />
      )}
    </button>
  );
}
