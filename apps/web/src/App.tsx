import type { LiveConnection } from "./useFirestore";
import { Workspace } from "./app/Workspace";
import { DemoApp } from "./demo/DemoApp";

/** Select the connected workspace or the explicitly requested sample app. */
export default function App({ connection }: { connection?: LiveConnection }) {
  return connection ? <Workspace connection={connection} /> : <DemoApp />;
}
