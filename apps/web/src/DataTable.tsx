import type { ComponentProps } from "react";

/** Shared scrolling, typography and density; callers keep native table semantics. */
export function DataTable({
  className = "",
  children,
  ...props
}: ComponentProps<"table">) {
  return (
    <div className="table-scroll">
      <table className={`data-table ${className}`} {...props}>
        {children}
      </table>
    </div>
  );
}
