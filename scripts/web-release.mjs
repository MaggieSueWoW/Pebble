#!/usr/bin/env node
// Explicit Firebase Hosting releases; no worker, Firestore or IAM mutations.
import { createHash } from "node:crypto";
import {
  cpSync,
  lstatSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { execFileSync } from "node:child_process";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const dist = join(root, "apps/web/dist");
const stage = join(root, "deploy/private/web-release");
const digest = (value) => createHash("sha256").update(value).digest("hex");
const readJSON = (path) => JSON.parse(readFileSync(path, "utf8"));
const run = (command, args, options = {}) =>
  execFileSync(command, args, { cwd: root, stdio: "inherit", ...options });
const git = (...args) =>
  execFileSync("git", args, { cwd: root, encoding: "utf8" }).trim();
const keys = [
  "environment",
  "projectId",
  "siteId",
  "apiKey",
  "authDomain",
  "appId",
  "seasonId",
  "seasonLabel",
];

export function validateConfig(config) {
  if (
    !config ||
    Object.keys(config).sort().join() !== [...keys].sort().join() ||
    keys.some((key) => typeof config[key] !== "string" || !config[key])
  )
    throw new Error(
      "Use only the public fields in deploy/web-release.example.json",
    );
  if (!["live", "test"].includes(config.environment))
    throw new Error("Choose live or test environment explicitly");
  if (
    !/^[a-z][a-z0-9-]{4,28}[a-z0-9]$/.test(config.projectId) ||
    config.projectId.startsWith("demo-")
  )
    throw new Error(
      "Choose an actual Firebase project; demo projects are emulator-only",
    );
  if (!/^[a-z0-9][a-z0-9-]{0,61}[a-z0-9]$/.test(config.siteId))
    throw new Error("Invalid Hosting site ID");
  if (
    !/^AIza[A-Za-z0-9_-]{35}$/.test(config.apiKey) ||
    !/^1:\d+:web:[a-f0-9]+$/i.test(config.appId)
  )
    throw new Error(
      "Use Firebase web app configuration, never a server credential",
    );
  if (
    !/^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$/.test(config.authDomain) ||
    !config.authDomain.includes(".")
  )
    throw new Error("authDomain must be a hostname without a scheme or path");
  if (
    !/^[A-Za-z0-9_-]{1,100}$/.test(config.seasonId) ||
    config.seasonLabel.length > 100
  )
    throw new Error("Invalid season configuration");
  return Object.fromEntries(keys.map((key) => [key, config[key]]));
}

export function publicEnv(config, inherited = process.env) {
  const env = Object.fromEntries(
    Object.entries(inherited).filter(([key]) => !key.startsWith("VITE_")),
  );
  return {
    ...env,
    PEBBLE_WEB_RELEASE_BUILD: "1",
    VITE_FIREBASE_PROJECT_ID: config.projectId,
    VITE_FIREBASE_API_KEY: config.apiKey,
    VITE_FIREBASE_AUTH_DOMAIN: config.authDomain,
    VITE_FIREBASE_APP_ID: config.appId,
    VITE_PEBBLE_SEASON_ID: config.seasonId,
    VITE_PEBBLE_SEASON_LABEL: config.seasonLabel,
    VITE_FIREBASE_USE_EMULATORS: "false",
  };
}

export function fileHashes(directory, prefix = "") {
  const result = {};
  for (const name of readdirSync(directory).sort()) {
    const path = join(directory, name),
      relative = prefix + name,
      stat = lstatSync(path);
    if (stat.isSymbolicLink())
      throw new Error("Release cannot include symlinks");
    if (stat.isDirectory())
      Object.assign(result, fileHashes(path, relative + "/"));
    else if (stat.isFile() && relative !== "release.json") {
      const bytes = readFileSync(path);
      if (
        /-----BEGIN [A-Z ]*PRIVATE KEY-----|["']private_key["']\s*:|["']client_secret["']\s*:|WCL_CLIENT_SECRET/.test(
          bytes.toString("utf8"),
        )
      )
        throw new Error("Possible server credential in release output");
      result[relative] = digest(bytes);
    }
  }
  return result;
}

export function verifyArtifact(directory, config, revision) {
  const manifest = readJSON(join(directory, "release.json"));
  if (
    Object.keys(manifest).sort().join() !==
      "configHash,environment,files,revision" ||
    manifest.environment !== config.environment ||
    manifest.revision !== revision ||
    manifest.configHash !== digest(JSON.stringify(config))
  )
    throw new Error(
      "Release source/config does not match this checkout and target; rebuild",
    );
  const files = fileHashes(directory);
  if (
    !files["index.html"] ||
    !Object.keys(files).some((path) => /^assets\/.*\.js$/.test(path)) ||
    JSON.stringify(files) !== JSON.stringify(manifest.files)
  )
    throw new Error("Release files changed after build; rebuild");
  return manifest;
}

export function firebaseArgs(action, config, configPath, channel) {
  const common = [
    "--project",
    config.projectId,
    "--config",
    configPath,
    "--non-interactive",
  ];
  if (action === "deploy") {
    if (channel !== undefined)
      throw new Error("Live deploy does not accept a preview channel");
    return ["deploy", "--only", "hosting", ...common];
  }
  if (
    action !== "preview" ||
    !/^[a-z][a-z0-9-]{0,29}$/.test(channel ?? "") ||
    channel === "live"
  )
    throw new Error("Preview requires a non-live channel name");
  // Domain changes belong to the explicit Auth administrator acceptance step.
  return [
    "hosting:channel:deploy",
    channel,
    "--expires",
    "7d",
    "--no-authorized-domains",
    ...common,
  ];
}

function requireClean() {
  if (git("status", "--porcelain", "--untracked-files=normal"))
    throw new Error(
      "Commit changes first so the release revision identifies its source",
    );
}

async function main() {
  const [configFile, action, channel, ...extra] = process.argv.slice(2);
  if (
    !configFile ||
    extra.length ||
    (action !== "preview" && channel !== undefined) ||
    !["build", "check", "preview", "deploy"].includes(action)
  )
    throw new Error(
      "Usage: node scripts/web-release.mjs PRIVATE_CONFIG {build|check|preview CHANNEL|deploy}",
    );
  const config = validateConfig(readJSON(resolve(configFile)));
  const revision = git("rev-parse", "HEAD");
  requireClean();
  if (action === "build") {
    run("npm", ["--prefix", "apps/web", "run", "build"], {
      env: publicEnv(config),
    });
    const manifest = {
      revision,
      environment: config.environment,
      configHash: digest(JSON.stringify(config)),
      files: fileHashes(dist),
    };
    writeFileSync(
      join(dist, "release.json"),
      JSON.stringify(manifest, null, 2) + "\n",
    );
  }
  verifyArtifact(dist, config, revision);
  if (action === "preview" || action === "deploy") {
    const args = firebaseArgs(
      action,
      config,
      join(stage, "firebase.json"),
      channel,
    );
    rmSync(stage, { recursive: true, force: true });
    mkdirSync(stage, { recursive: true });
    cpSync(dist, join(stage, "public"), { recursive: true });
    verifyArtifact(join(stage, "public"), config, revision);
    const hosting = {
      ...readJSON(join(root, "firebase.json")).hosting,
      site: config.siteId,
      public: "public",
    };
    writeFileSync(
      join(stage, "firebase.json"),
      JSON.stringify({ hosting }, null, 2) + "\n",
    );
    run("firebase", args);
  }
  console.log(
    `Hosting ${action} verified for source ${revision} (${config.environment}); worker and data services are outside this command.`,
  );
}

if (
  process.argv[1] &&
  resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  main().catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
}
