import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const script = fileURLToPath(new URL("./deploy-officers.sh", import.meta.url));
test("first deployment permits Firebase to configure cleanup, then enforces one-day retention", () => {
  const directory = mkdtempSync(join(tmpdir(), "officer-deploy-"));
  try {
    const log = join(directory, "calls");
    // Model the first-deployment failure: Firebase requires --force to create
    // its initial cleanup policy instead of exiting before the next command.
    writeFileSync(
      join(directory, "firebase"),
      '#!/bin/sh\nprintf "%s\\n" "$*" >> "$CALL_LOG"\nif [ "$1" = deploy ]; then\n  case " $* " in *" --force "*) ;; *) exit 17 ;; esac\nfi\n',
      { mode: 0o700 },
    );
    const result = spawnSync("bash", [script, "example-project"], {
      encoding: "utf8",
      env: {
        ...process.env,
        PATH: `${directory}:${process.env.PATH}`,
        CALL_LOG: log,
      },
    });
    assert.equal(result.status, 0, result.stderr);
    assert.deepEqual(readFileSync(log, "utf8").trim().split("\n"), [
      "deploy --project example-project --only functions:officers,firestore:rules --force",
      "functions:artifacts:setpolicy --project example-project --location us-central1 --days 1 --force",
    ]);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
