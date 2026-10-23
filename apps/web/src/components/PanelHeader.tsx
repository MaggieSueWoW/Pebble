import type { ReactNode } from "react";

export function PanelHeader({
  title,
  id,
  description,
  actions,
}: {
  title: ReactNode;
  id?: string;
  description?: string;
  actions?: ReactNode;
}) {
  return (
    <div className="panel-header">
      <div>
        <h2 id={id}>{title}</h2>
        {description && <p>{description}</p>}
      </div>
      {actions}
    </div>
  );
}
