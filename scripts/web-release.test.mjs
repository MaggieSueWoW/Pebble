import test from "node:test";
import assert from "node:assert/strict";
import {
  mkdtempSync,
  writeFileSync,
  mkdirSync,
  symlinkSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import {
  validateConfig,
  publicEnv,
  fileHashes,
  verifyArtifact,
  firebaseArgs,
  verifySite,
} from "./web-release.mjs";
const config = {
  environment: "test",
  projectId: "pebble-hosting-check",
  siteId: "pebble-hosting-check",
  apiKey: "AIza" + "A".repeat(35),
  authDomain: "pebble-hosting-check.firebaseapp.com",
  appId: "1:123:web:abcdef",
  seasonId: "test",
  seasonLabel: "Test",
};
const hash = (value) => createHash("sha256").update(value).digest("hex");
test("release environment rejects server configuration, placeholders and demo/emulator targets", () => {
  assert.deepEqual(validateConfig(config), config);
  for (const change of [
    { private_key: "secret" },
    { credentials_file: "server.json" },
    { useEmulators: true },
    { projectId: "demo-pebble" },
    { apiKey: "replace-me" },
    { environment: "" },
    { authDomain: "https://host.example/path" },
  ])
    assert.throws(() => validateConfig({ ...config, ...change }));
  const env = publicEnv(config, {
    PATH: "tools",
    VITE_WCL_CLIENT_SECRET: "must-not-leak",
    VITE_FIREBASE_USE_EMULATORS: "true",
    VITE_FIRESTORE_EMULATOR_PORT: "9999",
  });
  assert.equal(env.VITE_WCL_CLIENT_SECRET, undefined);
  assert.equal(env.VITE_FIRESTORE_EMULATOR_PORT, undefined);
  assert.equal(env.VITE_FIREBASE_USE_EMULATORS, "false");
  assert.equal(env.PEBBLE_WEB_RELEASE_BUILD, "1");
});
test("release target is explicit and commands only deploy Hosting without changing Auth domains", () => {
  assert.throws(() =>
    firebaseArgs("deploy", config, "release/firebase.json", "review"),
  );
  const args = firebaseArgs("deploy", config, "release/firebase.json");
  assert.deepEqual(args, [
    "deploy",
    "--only",
    "hosting",
    "--project",
    config.projectId,
    "--config",
    "release/firebase.json",
    "--non-interactive",
  ]);
  assert(
    firebaseArgs("preview", config, "release/firebase.json", "review").includes(
      "--no-authorized-domains",
    ),
  );
  for (const channel of ["live", "../live", "review;other", undefined])
    assert.throws(() =>
      firebaseArgs("preview", config, "release/firebase.json", channel),
    );
});
test("release verification rejects modified, extra, secret and symlinked files and target drift", () => {
  const dir = mkdtempSync(join(tmpdir(), "pebble-release-"));
  const revision = "a".repeat(40);
  try {
    mkdirSync(join(dir, "assets"));
    writeFileSync(
      join(dir, "index.html"),
      '<script src="/assets/index-hash.js"></script>',
    );
    writeFileSync(join(dir, "assets/index-hash.js"), 'console.log("public");');
    const manifest = {
      revision,
      environment: config.environment,
      configHash: hash(JSON.stringify(config)),
      files: fileHashes(dir),
    };
    writeFileSync(join(dir, "release.json"), JSON.stringify(manifest));
    assert.equal(verifyArtifact(dir, config, revision).revision, revision);
    assert.throws(() =>
      verifyArtifact(dir, { ...config, seasonId: "another" }, revision),
    );
    assert.throws(() => verifyArtifact(dir, config, "b".repeat(40)));
    writeFileSync(join(dir, "extra.json"), "{}");
    assert.throws(() => verifyArtifact(dir, config, revision));
    rmSync(join(dir, "extra.json"));
    writeFileSync(join(dir, "assets/index-hash.js"), "changed");
    assert.throws(() => verifyArtifact(dir, config, revision));
    writeFileSync(join(dir, "secret.json"), '{"private_key":"secret"}');
    assert.throws(() => fileHashes(dir), /credential/);
    rmSync(join(dir, "secret.json"));
    symlinkSync("index.html", join(dir, "linked.html"));
    assert.throws(() => fileHashes(dir), /symlink/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("publishing rejects a Hosting site outside the selected project", () => {
  verifySite(config, {
    status: "success",
    result: { sites: [{ name: "projects/example/sites/" + config.siteId }] },
  });
  for (const result of [
    {},
    { status: "error" },
    {
      status: "success",
      result: { sites: [{ name: "projects/other/sites/wrong-site" }] },
    },
  ])
    assert.throws(() => verifySite(config, result));
});
