import { after, before, beforeEach, test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { assertFails, assertSucceeds, initializeTestEnvironment } from '@firebase/rules-unit-testing';
import { collection, doc, getDoc, getDocs, getDocFromServer, getDocsFromServer, serverTimestamp, setDoc, updateDoc, deleteDoc, writeBatch } from 'firebase/firestore';
let env;
const [emulatorHost, emulatorPort] = (process.env.FIRESTORE_EMULATOR_HOST || '127.0.0.1:8080').split(':');
const season = 'seasons/midnight-s2';
const code = 'aB3dE6gH9jK2mN5p';
const meta = (uid = 'officer', version = 1) => ({ version, updatedBy: uid, updatedAt: serverTimestamp() });
// Model browser linked writes: the main version is part of the same commit.
async function linkedWrite(db, ref, operation, data, playerIds = ['p1']) {
  const batch = writeBatch(db);
  for (const id of new Set(playerIds)) {
    const main = doc(db, `${season}/players/${id}`);
    const snapshot = await getDoc(main);
    batch.update(main, meta('officer', snapshot.data().version + 1));
  }
  if (operation === 'delete') batch.delete(ref);
  else batch[operation](ref, data);
  return batch.commit();
}
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
  await assertSucceeds(deleteDoc(ref));
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
  await assertSucceeds(linkedWrite(db, ref, 'set', base));
  await assertFails(linkedWrite(db, ref, 'update', { post: { mode: 'partial', minutes: 0 }, ...meta('officer', 2) }));
  await assertFails(linkedWrite(db, ref, 'update', { pre: { mode: 'automatic', minutes: 5 }, ...meta('officer', 2) }));
  await assertFails(linkedWrite(db, doc(db, `${season}/availabilityOverrides/wrong-id`), 'set', base));
  await assertFails(linkedWrite(db, doc(db, `${season}/availabilityOverrides/2026-10-15_missing`), 'set', { ...base, playerId: 'missing' }));
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

const families = {
  reports: [code, { enabled: true, notes: '' }],
  players: ['p2', { name: 'Exampletwo', className: 'Druid', role: 'Healer', active: true, joinDate: '2026-10-01', leaveDate: '2026-11-01', notes: '' }],
  characters: ['c1', { name: 'Examplealt', realm: 'Test Realm', region: 'us', playerId: 'p1' }],
  nightOverrides: ['2026-10-15', { start: '2026-10-15T19:00', breakStart: '2026-10-15T20:00', breakEnd: '2026-10-15T20:15', end: '2026-10-15T21:00', notes: '' }],
  availabilityOverrides: ['2026-10-15_p1', { nightId: '2026-10-15', playerId: 'p1', pre: { mode: 'available' }, post: { mode: 'unavailableFor', minutes: 15 }, notes: '' }],
  settings: ['calculation', { tz: 'America/Los_Angeles', breakWindowStart: '20:50', breakWindowEnd: '21:30', minBreakMinutes: 10, maxBreakMinutes: 30, defaultMythicStart: '', postExtensionMinutes: 5 }],
};
for (const [family, [id, domain]] of Object.entries(families)) {
  test(`${family}: full write contract, forged metadata, stale versions and revocation`, async () => {
    const db = env.authenticatedContext('officer').firestore();
    const ref = doc(db, `${season}/${family}/${id}`);
    const linked = ['characters', 'availabilityOverrides'].includes(family);
    const write = (operation, data) => linked ? linkedWrite(db, ref, operation, data) : operation === 'delete' ? deleteDoc(ref) : operation === 'set' ? setDoc(ref, data) : updateDoc(ref, data);
    await assertSucceeds(write('set', { ...domain, ...meta() }));
    for (const change of [meta('officer', 1), meta('officer', 3), { ...meta('other', 2) }, { ...meta('officer', 2), updatedAt: new Date(0) }, { ...meta('officer', 2), version: 2.5 }, { ...meta('officer', 2), derived: 'forged' }]) {
      await assertFails(write('update', change));
    }
    await assertSucceeds(write('update', meta('officer', 2)));
    if (['reports', 'players', 'characters'].includes(family)) await assertSucceeds(write('delete'));
    else await assertFails(deleteDoc(ref));
    for (const reader of [env.unauthenticatedContext().firestore(), env.authenticatedContext('member').firestore()]) {
      await assertFails(getDocFromServer(doc(reader, `${season}/${family}/${id}`)));
      await assertFails(getDocsFromServer(collection(reader, `${season}/${family}`)));
      await assertFails(deleteDoc(doc(reader, `${season}/${family}/${id}`)));
      await assertFails(setDoc(doc(reader, `${season}/${family}/${id}`), { ...domain, ...meta('member', 3) }));
    }
    await env.withSecurityRulesDisabled(ctx => setDoc(doc(ctx.firestore(), 'officers/officer'), { enabled: false }));
    await assertFails(updateDoc(ref, meta('officer', 3)));
    await assertFails(deleteDoc(ref));
    await assertFails(getDocFromServer(ref));
  });
}

test('roster and character shape, enums, references and calendar-shaped ordering', async () => {
  const db = env.authenticatedContext('officer').firestore();
  const [pid, player] = families.players;
  const pref = doc(db, `${season}/players/${pid}`);
  for (const invalid of [{ active: 'true' }, { className: 'Wizard' }, { role: 'Officer' }, { name: '' }, { name: 'X'.repeat(33) }, { joinDate: '2026-13-01' }, { leaveDate: '2026-09-01' }, { notes: 'x'.repeat(1001) }]) {
    await assertFails(setDoc(pref, { ...player, ...invalid, ...meta() }));
  }
  const [cid, character] = families.characters;
  for (const invalid of [{ region: 'US' }, { playerId: 'missing' }, { playerId: '' }, { playerId: 'p1/subpath' }, { name: '' }, { realm: 'x'.repeat(101) }]) {
    await assertFails(linkedWrite(db, doc(db, `${season}/characters/${cid}`), 'set', { ...character, ...invalid, ...meta() }));
  }
  await assertFails(setDoc(doc(db, `${season}/players/invalid.id`), { ...player, ...meta() }));
});

test('calculation settings reject type/range/order violations and unknown documents', async () => {
  const [id, settings] = families.settings;
  const db = env.authenticatedContext('officer').firestore();
  for (const invalid of [{ tz: 'UTC' }, { breakWindowStart: '' }, { breakWindowEnd: '25:00' }, { breakWindowStart: '22:00' }, { minBreakMinutes: 0 }, { maxBreakMinutes: 121 }, { minBreakMinutes: 31 }, { defaultMythicStart: '7 PM' }, { postExtensionMinutes: -1 }, { postExtensionMinutes: 61 }, { postExtensionMinutes: '5' }]) {
    await assertFails(setDoc(doc(db, `${season}/settings/${id}`), { ...settings, ...invalid, ...meta() }));
  }
  await assertFails(setDoc(doc(db, `${season}/settings/unknown`), { ...settings, ...meta() }));
});

test('timing requires ordered explicit boundaries; worker owns actual calendar/DST validation', async () => {
  const [id, timing] = families.nightOverrides;
  const db = env.authenticatedContext('officer').firestore();
  for (const invalid of [{ end: '2026-10-15T18:00' }, { breakEnd: '2026-10-15T19:30' }, { start: '2026-10-15T25:00' }, { breakStart: '' }]) {
    await assertFails(setDoc(doc(db, `${season}/nightOverrides/${id}`), { ...timing, ...invalid, ...meta() }));
  }
  // Rules validate shape and lexical order, not leap years or DST: SDK/worker
  // validation is still mandatory even when browser writes passed these rules.
  await assertSucceeds(setDoc(doc(db, `${season}/nightOverrides/2026-02-30`), { start: '2026-02-30T19:00', breakStart: '', breakEnd: '', end: '', ...meta() }));
});

test('no browser can write any output family; public cannot enumerate roots or read absent-parent staging', async () => {
  const db = env.authenticatedContext('officer').firestore();
  for (const child of ['players/p1', 'nights/2026-10-15', 'weeks/2026-10-13']) {
    await assertFails(setDoc(doc(db, `${season}/results/forged/${child}`), { complete: true }));
  }
  await env.withSecurityRulesDisabled(async ctx => {
    for (const child of ['players/p1', 'nights/2026-10-15', 'weeks/2026-10-13']) {
      await setDoc(doc(ctx.firestore(), `${season}/results/orphan/${child}`), { note: 'private staging' });
      await setDoc(doc(ctx.firestore(), `${season}/results/ready/${child}`), { played: 1 });
    }
    await setDoc(doc(ctx.firestore(), `${season}/results/ready`), { complete: true });
  });
  const anon = env.unauthenticatedContext().firestore();
  await assertFails(getDocsFromServer(collection(anon, `${season}/results`)));
  for (const family of ['players', 'nights', 'weeks']) {
    await assertFails(getDocsFromServer(collection(anon, `${season}/results/orphan/${family}`)));
    await assertSucceeds(getDocsFromServer(collection(anon, `${season}/results/ready/${family}`)));
  }
});


test('recorded availability stays public while night diagnostics require current officer access', async () => {
  const runPath = `${season}/runs/provenance`;
  await env.withSecurityRulesDisabled(async ctx => {
    await setDoc(doc(ctx.firestore(), runPath), { status: 'succeeded', nightDiagnostics: { '2026-10-15': { reportCount: 1, mythicFights: 2, unmappedCharacters: 0, breakMinutes: null, timing: { start: 'detected', end: 'detected', break: 'none' } } } });
    await setDoc(doc(ctx.firestore(), `${season}/results/provenance`), { complete: true });
    await setDoc(doc(ctx.firestore(), `${season}/results/provenance/nights/2026-10-15`), { players: [{ id: 'p1', availability: { pre: { available: true, mode: 'partial' }, post: { available: false, mode: 'automatic' }, source: 'override' } }] });
  });
  const officer = env.authenticatedContext('officer').firestore();
  await assertSucceeds(getDoc(doc(officer, runPath)));
  for (const reader of [env.unauthenticatedContext(), env.authenticatedContext('member')]) {
    await assertFails(getDoc(doc(reader.firestore(), runPath)));
    await assertSucceeds(getDoc(doc(reader.firestore(), `${season}/results/provenance/nights/2026-10-15`)));
  }
  await env.withSecurityRulesDisabled(async ctx => { await setDoc(doc(ctx.firestore(), 'officers/officer'), { enabled: false }); });
  await assertFails(getDocFromServer(doc(officer, runPath)));
});

test('only officers can delete a main and linked records together', async () => {
  await env.withSecurityRulesDisabled(async ctx => {
    const db = ctx.firestore();
    await setDoc(doc(db, `${season}/characters/alt`), {name: 'Alt', realm: '', region: 'us', playerId: 'p1', version: 1});
    await setDoc(doc(db, `${season}/availabilityOverrides/2026-10-15_p1`), {nightId:'2026-10-15', playerId:'p1', pre:{mode:'automatic'}, post:{mode:'unavailable'}, version:1});
  });
  const paths = ['players/p1', 'characters/alt', 'availabilityOverrides/2026-10-15_p1'];
  for (const db of [env.unauthenticatedContext().firestore(), env.authenticatedContext('member').firestore()]) {
    const batch = writeBatch(db); paths.forEach(path => batch.delete(doc(db, `${season}/${path}`)));
    await assertFails(batch.commit());
  }
  const db = env.authenticatedContext('officer').firestore();
  const batch = writeBatch(db); paths.forEach(path => batch.delete(doc(db, `${season}/${path}`)));
  await assertSucceeds(batch.commit());
  for (const path of paths) assert.equal((await getDoc(doc(db, `${season}/${path}`))).exists(), false);
});

test('officers can remove one alt without removing its main', async () => {
  await env.withSecurityRulesDisabled(ctx => setDoc(doc(ctx.firestore(), `${season}/characters/alt`), {name:'Alt', realm:'', region:'us', playerId:'p1', version:1}));
  for (const reader of [env.unauthenticatedContext().firestore(), env.authenticatedContext('member').firestore()]) await assertFails(deleteDoc(doc(reader, `${season}/characters/alt`)));
  const db = env.authenticatedContext('officer').firestore();
  await assertFails(deleteDoc(doc(db, `${season}/characters/alt`)));
  await assertSucceeds(linkedWrite(db, doc(db, `${season}/characters/alt`), 'delete'));
  assert.equal((await getDoc(doc(db, `${season}/players/p1`))).exists(), true);
});


test('linked writes require an atomic main version advance, including both sides of reassignment', async () => {
  const db = env.authenticatedContext('officer').firestore();
  const ref = doc(db, `${season}/characters/alt`);
  const character = {name:'Alt', realm:'', region:'us', playerId:'p1', ...meta()};
  const availability = doc(db, `${season}/availabilityOverrides/2026-10-15_p1`);
  const adjustment = {nightId:'2026-10-15', playerId:'p1', pre:{mode:'automatic'}, post:{mode:'available'}, ...meta()};
  await assertFails(setDoc(ref, character));
  await assertFails(setDoc(availability, adjustment));
  await assertSucceeds(linkedWrite(db, ref, 'set', character));
  await assertSucceeds(linkedWrite(db, availability, 'set', adjustment));
  await assertFails(updateDoc(ref, {realm:'Changed', ...meta('officer', 2)}));
  await assertFails(updateDoc(availability, {notes:'Changed', ...meta('officer', 2)}));
  await assertFails(deleteDoc(ref));
  await assertSucceeds(linkedWrite(db, doc(db, `${season}/players/p2`), 'set', {...families.players[1], ...meta()}, []));
  await assertFails(linkedWrite(db, ref, 'update', {playerId:'p2', ...meta('officer', 2)}, ['p2']));
  await assertFails(linkedWrite(db, ref, 'update', {playerId:'p2', ...meta('officer', 2)}, ['p1']));
  await assertSucceeds(linkedWrite(db, ref, 'update', {playerId:'p2', ...meta('officer', 2)}, ['p1', 'p2']));
  assert.equal((await getDoc(doc(db, `${season}/players/p1`))).data().version, 4);
  assert.equal((await getDoc(doc(db, `${season}/players/p2`))).data().version, 2);
});

test('a main cannot disappear in the same commit that adds or reassigns a surviving link', async () => {
  const db = env.authenticatedContext('officer').firestore();
  for (const [family, id, value] of [
    ['characters', 'alt', {name:'Alt', realm:'', region:'us', playerId:'p1'}],
    ['availabilityOverrides', '2026-10-15_p1', {nightId:'2026-10-15', playerId:'p1', pre:{mode:'automatic'}, post:{mode:'available'}}],
  ]) {
    const batch = writeBatch(db);
    batch.delete(doc(db, `${season}/players/p1`));
    batch.set(doc(db, `${season}/${family}/${id}`), {...value, ...meta()});
    await assertFails(batch.commit());
    assert.equal((await getDoc(doc(db, `${season}/players/p1`))).exists(), true);
  }
  await assertSucceeds(deleteDoc(doc(db, `${season}/players/p1`)));
  await assertFails(setDoc(doc(db, `${season}/characters/late`), {name:'Late', realm:'', region:'us', playerId:'p1', ...meta()}));
  await assertFails(setDoc(doc(db, `${season}/availabilityOverrides/2026-10-15_p1`), {nightId:'2026-10-15', playerId:'p1', pre:{mode:'automatic'}, post:{mode:'available'}, ...meta()}));
});

test('officers can persist season ignores without a main; public, forged and linked bypasses fail', async () => {
  const db = env.authenticatedContext('officer').firestore();
  const ref = doc(db, `${season}/characters/ignore-extra`);
  const value = { name: 'Extra', realm: 'Azjol-Nerub', region: 'eu', playerId: '', ignored: true, ...meta() };
  for (const reader of [env.unauthenticatedContext().firestore(), env.authenticatedContext('member').firestore()]) await assertFails(setDoc(doc(reader, `${season}/characters/ignore-extra`), { ...value, ...meta('member') }));
  await assertSucceeds(setDoc(ref, value));
  for (const reader of [env.unauthenticatedContext().firestore(), env.authenticatedContext('member').firestore()]) {
    await assertFails(getDoc(doc(reader, `${season}/characters/ignore-extra`)));
    await assertFails(deleteDoc(doc(reader, `${season}/characters/ignore-extra`)));
  }
  await assertFails(updateDoc(ref, { playerId: 'p1', ...meta('officer', 2) }));
  await assertFails(updateDoc(ref, { ignored: false, playerId: 'p1', ...meta('officer', 2) }));
  await assertSucceeds(deleteDoc(ref));
  await assertSucceeds(linkedWrite(db, ref, 'set', { name: 'Extra', realm: '', region: 'us', playerId: 'p1', ...meta() }));
  await assertFails(updateDoc(ref, { ignored: true, playerId: '', ...meta('officer', 2) }));
});
