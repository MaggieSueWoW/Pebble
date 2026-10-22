import { ChevronDown } from "lucide-react";
import type { ComponentProps } from "react";

export function SelectControl(props: ComponentProps<"select">) {
  return (
    <span className="select-control">
      <select {...props} />
      <ChevronDown size={16} aria-hidden="true" />
    </span>
  );
}
