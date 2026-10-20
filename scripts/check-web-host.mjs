#!/usr/bin/env node
// Read-only HTTP checks. This does not sign in, change data or queue a run.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";

const [origin, revision] = process.argv.slice(2);
if (!origin || !/^[0-9a-f]{40}$/.test(revision ?? ""))
  throw new Error(
    "Usage: node scripts/check-web-host.mjs ORIGIN EXPECTED_REVISION",
  );
const base = new URL(origin);
assert(
  base.pathname === "/" && !base.search && !base.hash,
  "Use a site origin",
);
assert(
  base.protocol === "https:" ||
    (base.protocol === "http:" &&
      ["localhost", "127.0.0.1"].includes(base.hostname)),
  "Use HTTPS or a local Hosting emulator",
);
async function get(path) {
  const response = await fetch(new URL(path, base), {
    redirect: "error",
    signal: AbortSignal.timeout(15000),
  });
  assert.equal(response.status, 200, `HTTP failure for ${path}`);
  return response;
}
const root = await get("/");
const html = await root.text();
assert.match(root.headers.get("cache-control") ?? "", /no-cache/);
const nested = await get("/reports");
assert.equal(
  await nested.text(),
  html,
  "SPA deep link must serve the same entrypoint",
);
assert.match(nested.headers.get("cache-control") ?? "", /no-cache/);
const metadata = await get("/release.json");
assert.match(metadata.headers.get("cache-control") ?? "", /no-cache/);
const manifest = await metadata.json();
assert.equal(manifest.revision, revision, "Wrong frontend revision");
const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");
assert.equal(hash(html), manifest.files["index.html"], "Stale entrypoint");
const asset = html.match(/(?:src|href)="([^\"]*\/assets\/[^\"]+\.js)"/)?.[1];
assert(asset, "No hashed JavaScript asset in entrypoint");
const js = await get(asset);
assert.match(js.headers.get("cache-control") ?? "", /max-age=31536000/);
assert.match(js.headers.get("cache-control") ?? "", /immutable/);
assert.match(js.headers.get("content-type") ?? "", /javascript/);
const assetPath = new URL(asset, base).pathname.slice(1);
assert.equal(
  hash(Buffer.from(await js.arrayBuffer())),
  manifest.files[assetPath],
  "Stale or modified asset",
);
console.log(
  "HTTPS/local host, SPA route, revision and entrypoint/asset cache headers passed. Auth and worker acceptance are separate.",
);
