import { after, before, beforeEach, test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { assertFails, assertSucceeds, initializeTestEnvironment } from '@firebase/rules-unit-testing';
import { collection, doc, getDoc, getDocs, getDocFromServer, getDocsFromServer, serverTimestamp, setDoc, updateDoc, deleteDoc } from 'firebase/firestore';
let env;
const [emulatorHost, emulatorPort] = (process.env.FIRESTORE_EMULATOR_HOST || '127.0.0.1:8080').split(':');
const season = 'seasons/midnight-s2';
const code = 'aB3dE6gH9jK2mN5p';
const meta = (uid = 'officer', version = 1) => ({ version, updatedBy: uid, updatedAt: serverTimestamp() });
before(async () => { env = await initializeTestEnvironment({ projectId: process.env.PEBBLE_RULES_TEST_PROJECT || 'demo-pebble', firestore: { host: emulatorHost, port: Number(emulatorPort), rules: await readFile(new URL('../firestore.rules', import.meta.url), 'utf8') } }); });
after(async () => env?.cleanup());
beforeEach(async () => {
  await env.clearFirestore();
  await env.withSecurityRulesDisabled(async ctx => {
    const db = ctx.firestore();
    await setDoc(doc(db, 'officers/officer'), { enabled: true });
    await setDoc(doc(db, `${season}/players/p1`), { name: 'Example', className: 'Druid', role: 'Healer', active: true, joinDate: '', leaveDate: '', ...meta() });
  });
});
test('only officers may save reports, with strict fields and attribution', async () => {
  const db = env.authenticatedContext('officer').firestore();
  const ref = doc(db, `${season}/reports/${code}`);
  await assertSucceeds(setDoc(ref, { enabled: true, notes: '', ...meta() }));
  await assertFails(updateDoc(ref, { enabled: false, ...meta('officer', 1) }));
  await assertSucceeds(updateDoc(ref, { enabled: false, ...meta('officer', 2) }));
  await assertFails(updateDoc(ref, { title: 'Spoofed derived field', ...meta('officer', 3) }));
  await assertFails(updateDoc(ref, { ...meta('someone-else', 3) }));
  await assertFails(deleteDoc(ref));
  await assertFails(setDoc(doc(env.unauthenticatedContext().firestore(), `${season}/reports/${code}`), { enabled: true, ...meta() }));
  await assertFails(setDoc(doc(env.authenticatedContext('member').firestore(), `${season}/reports/${code}`), { enabled: true, ...meta('member') }));
});
test('officer membership, worker status, results, and public pointer cannot be written by browsers', async () => {
  const db = env.authenticatedContext('officer').firestore();
  for (const path of ['officers/member', 'bot/status', 'publicSeasons/midnight-s2', `${season}/results/forged`, `${season}/results/forged/players/p1`]) await assertFails(setDoc(doc(db, path), { enabled: true, complete: true }));
  await assertFails(getDoc(doc(db, 'officers/another-officer')));
  await assertSucceeds(getDoc(doc(db, 'officers/officer')));
});
test('run submission is create-only and starts queued with authenticated identity', async () => {
  const db = env.authenticatedContext('officer').firestore();
  const ref = doc(db, `${season}/runs/run1`);
  await assertSucceeds(setDoc(ref, { status: 'queued', requestedBy: 'officer', requestedAt: serverTimestamp() }));
  await assertFails(updateDoc(ref, { status: 'succeeded' }));
  await assertFails(setDoc(doc(db, `${season}/runs/run2`), { status: 'running', requestedBy: 'officer', requestedAt: serverTimestamp() }));
  await assertFails(setDoc(doc(db, `${season}/runs/run2`), { status: 'queued', requestedBy: 'member', requestedAt: serverTimestamp() }));
  await assertFails(setDoc(doc(db, `${season}/runs/run2`), { status: 'queued', requestedBy: 'officer', requestedAt: serverTimestamp(), command: 'shell' }));
});
test('public readers can query completed results but never staging output or private data', async () => {
  await env.withSecurityRulesDisabled(async ctx => {
    for (const [id, complete] of [['ready', true], ['staging', false]]) {
      await setDoc(doc(ctx.firestore(), `${season}/results/${id}`), { complete });
      await setDoc(doc(ctx.firestore(), `${season}/results/${id}/players/p1`), { name: 'Example' });
    }
  });
  const db = env.unauthenticatedContext().firestore();
  await assertSucceeds(getDoc(doc(db, `${season}/results/ready`)));
  await assertSucceeds(getDocs(collection(db, `${season}/results/ready/players`)));
  await assertFails(getDoc(doc(db, `${season}/results/staging`)));
  await assertFails(getDocs(collection(db, `${season}/results/staging/players`)));
  for (const path of [`${season}/players/p1`, `${season}/runs/private`, 'bot/status']) await assertFails(getDoc(doc(db, path)));
});
test('availability has typed bounded minutes and a valid player identity', async () => {
  const db = env.authenticatedContext('officer').firestore();
  const base = { nightId: '2026-10-15', playerId: 'p1', pre: { mode: 'automatic' }, post: { mode: 'partial', minutes: 45 }, ...meta() };
  const ref = doc(db, `${season}/availabilityOverrides/2026-10-15_p1`);
  await assertSucceeds(setDoc(ref, base));
  await assertFails(updateDoc(ref, { post: { mode: 'partial', minutes: 0 }, ...meta('officer', 2) }));
  await assertFails(updateDoc(ref, { pre: { mode: 'automatic', minutes: 5 }, ...meta('officer', 2) }));
  await assertFails(setDoc(doc(db, `${season}/availabilityOverrides/wrong-id`), base));
  await assertFails(setDoc(doc(db, `${season}/availabilityOverrides/2026-10-15_missing`), { ...base, playerId: 'missing' }));
  assert.equal((await getDoc(ref)).data().post.minutes, 45);
});
test('timing requires a pair of explicit break boundaries', async () => {
  const db = env.authenticatedContext('officer').firestore();
  const ref = doc(db, `${season}/nightOverrides/2026-10-15`);
  await assertSucceeds(setDoc(ref, { start: '', breakStart: '', breakEnd: '', end: '', ...meta() }));
  await assertFails(updateDoc(ref, { breakStart: '2026-10-15T20:30', ...meta('officer', 2) }));
});

test('revocation removes private access from an existing session while completed results stay public', async () => {
  const db = env.authenticatedContext('officer').firestore();
  const report = doc(db, `${season}/reports/${code}`);
  await assertSucceeds(setDoc(report, { enabled: true, notes: 'private note', ...meta() }));
  await assertSucceeds(setDoc(doc(db, `${season}/runs/before-revocation`), { status: 'queued', requestedBy: 'officer', requestedAt: serverTimestamp() }));
  await env.withSecurityRulesDisabled(async ctx => {
    await setDoc(doc(ctx.firestore(), `${season}/results/ready`), { complete: true });
    await setDoc(doc(ctx.firestore(), `${season}/results/ready/players/p1`), { name: 'Example' });
    await setDoc(doc(ctx.firestore(), 'officers/officer'), { enabled: false });
  });
  // Server reads exercise the current rules, rather than previously cached data.
  for (const path of [`${season}/reports/${code}`, `${season}/players/p1`, `${season}/characters/alt`, `${season}/availabilityOverrides/private`, `${season}/nightOverrides/private`, `${season}/settings/calculation`, `${season}/runs/before-revocation`, 'bot/status']) {
    await assertFails(getDocFromServer(doc(db, path)));
  }
  await assertFails(getDocsFromServer(collection(db, `${season}/runs`)));
  await assertFails(updateDoc(report, { enabled: false, ...meta('officer', 2) }));
  await assertFails(setDoc(doc(db, `${season}/runs/after-revocation`), { status: 'queued', requestedBy: 'officer', requestedAt: serverTimestamp() }));
  await assertFails(setDoc(doc(db, 'officers/officer'), { enabled: true }));
  await assertSucceeds(getDocFromServer(doc(db, 'officers/officer')));
  for (const reader of [db, env.authenticatedContext('member').firestore(), env.unauthenticatedContext().firestore()]) {
    await assertSucceeds(getDocFromServer(doc(reader, `${season}/results/ready`)));
    await assertSucceeds(getDocsFromServer(collection(reader, `${season}/results/ready/players`)));
  }
  // Re-enabling membership restores access using the same signed-in identity.
  await env.withSecurityRulesDisabled(async ctx => {
    await setDoc(doc(ctx.firestore(), 'officers/officer'), { enabled: true });
  });
  await assertSucceeds(getDocFromServer(report));
});
