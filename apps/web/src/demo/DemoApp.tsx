import { Workspace } from "../app/Workspace";
import { DemoControls } from "./DemoControls";
import { useDemoSession } from "./useDemoSession";

export function DemoApp() {
  const demo = useDemoSession();
  return (
    <Workspace
      demo={demo}
      renderDemoControls={(onReset) => (
        <DemoControls session={demo} onReset={onReset} />
      )}
    />
  );
}
