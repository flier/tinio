#!/usr/bin/env node
// ci-analyze: per-run CI compile/cache report from GitHub Actions logs.
//
// Usage:
//   node ci-analyze.js <run-id> [<baseline-run-id>]
//
// Reads, for each run: the jobs API (names + wall times) and the logs zip
// (sccache `--show-stats` JSON per job). Prints a table of
// job | wall_s | compile_req | rust_hits | rust_hit% | write_errors,
// plus a totals row. With two run ids the second run's columns are shown
// as deltas vs the first, for before/after comparisons of a CI change.
//
// Requires: `gh` CLI authenticated with repo access; network for the
// logs download (export HTTP_PROXY/HTTPS_PROXY when needed). Local only —
// it is not part of the workflow.
'use strict';
const { execFileSync, spawnSync } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');

function gh(args, binary = false) {
  const out = spawnSync('gh', args, { encoding: binary ? null : 'utf8', maxBuffer: 1 << 30 });
  if (out.status !== 0) {
    throw new Error(`gh ${args.join(' ')} failed: ${(out.stderr || out.stdout || '').toString().slice(0, 500)}`);
  }
  return out.stdout;
}

const nameWithOwner = gh(['repo', 'view', '--json', 'nameWithOwner', '--jq', '.nameWithOwner']).trim();

function jobRows(runId) {
  const raw = gh(['api', `repos/${nameWithOwner}/actions/runs/${runId}/jobs`, '--paginate', '--jq',
    '.jobs[] | [.name, .conclusion, .started_at, .completed_at] | @tsv']);
  const rows = {};
  for (const line of raw.split('\n')) {
    if (!line.trim()) continue;
    const [name, conclusion, started, completed] = line.split('\t');
    const wall = started && completed ? Math.round((Date.parse(completed) - Date.parse(started)) / 1000) : null;
    rows[name.replace(/\//g, '_')] = { name, conclusion, wall };
  }
  return rows;
}

function extractLogs(runId) {
  const zip = path.join(os.tmpdir(), `ci-logs-${runId}.zip`);
  const dir = path.join(os.tmpdir(), `ci-logs-${runId}`);
  fs.writeFileSync(zip, gh(['api', `repos/${nameWithOwner}/actions/runs/${runId}/logs`], true));
  fs.rmSync(dir, { recursive: true, force: true });
  fs.mkdirSync(dir, { recursive: true });
  for (const args of [['-xf', zip, '-C', dir], ['-o', '-q', zip, '-d', dir]]) {
    const r = spawnSync('tar', args); // Windows bsdtar reads zip; busybox fallback below
    if (r.status === 0 && fs.readdirSync(dir).length > 0) return dir;
  }
  const r = spawnSync('unzip', ['-o', '-q', zip, '-d', dir]);
  if (r.status === 0) return dir;
  throw new Error('no zip extractor available (tried tar, unzip)');
}

function walk(dir, acc = []) {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) walk(p, acc);
    else if (e.name.endsWith('.txt')) acc.push(p);
  }
  return acc;
}

// Job key from a log filename: the zip uses literal '\' separators, so the
// first segment is the job name; root-level flat files are "<n>_<job>.txt".
function fileJobKey(name) {
  const seg = name.split('\\');
  const key = seg.length > 1 ? seg[0] : name.replace(/\.txt$/, '').replace(/^\d+_/, '');
  return key.replace(/\//g, '_');
}

function statsOf(txt) {
  const out = { req: 0, rustHits: 0, writeErr: 0, writes: 0 };
  for (const line of txt.split('\n')) {
    const i = line.indexOf('{"stats":');
    if (i < 0) continue;
    try {
      const st = JSON.parse(line.slice(i)).stats || {};
      const rh = (st.cache_hits && st.cache_hits.counts && st.cache_hits.counts.Rust) || 0;
      if ((st.compile_requests || 0) > out.req) { // per-file prints repeat; keep the largest
        out.req = st.compile_requests;
        out.rustHits = rh;
        out.writeErr = st.cache_write_errors || 0;
        out.writes = st.cache_writes || 0;
      }
    } catch { /* not a stats JSON line */ }
  }
  return out;
}

function parseRun(runId) {
  const jobs = jobRows(runId);
  const dir = extractLogs(runId);
  const perJob = {};
  for (const f of walk(dir)) {
    const key = fileJobKey(path.basename(f));
    const st = statsOf(fs.readFileSync(f, 'utf8'));
    if (!st.req) continue;
    const agg = perJob[key] || (perJob[key] = { req: 0, rustHits: 0, writeErr: 0, writes: 0 });
    for (const k of ['req', 'rustHits', 'writeErr', 'writes']) agg[k] = Math.max(agg[k], st[k]);
  }
  // join with jobs API (wall, conclusion) by normalized name
  const rows = [];
  for (const [key, st] of Object.entries(perJob)) {
    const j = jobs[key] || {};
    rows.push({ name: j.name || key, wall: j.wall ?? null, conclusion: j.conclusion ?? '', ...st });
  }
  rows.sort((a, b) => b.req - a.req);
  fs.rmSync(dir, { recursive: true, force: true });
  fs.rmSync(path.join(os.tmpdir(), `ci-logs-${runId}.zip`), { force: true });
  return rows;
}

function total(rows) {
  return rows.reduce((t, r) => ({ req: t.req + r.req, rustHits: t.rustHits + r.rustHits, writeErr: t.writeErr + r.writeErr }), { req: 0, rustHits: 0, writeErr: 0 });
}

function fmt(n) { return n == null ? '    -' : String(n).padStart(5); }
function fmtRate(h, r) { return r ? (100 * h / r).toFixed(1).padStart(6) + '%' : '    -'; }

function printRows(label, rows) {
  console.log(`\n${label}`);
  console.log('job'.padEnd(58), 'wall_s', 'req'.padStart(6), 'rustHit'.padStart(8), 'rate'.padStart(7), 'wErr'.padStart(6));
  for (const r of rows) {
    console.log(r.name.slice(0, 57).padEnd(58), fmt(r.wall), String(r.req).padStart(6), String(r.rustHits).padStart(8), fmtRate(r.rustHits, r.req), String(r.writeErr).padStart(6));
  }
  const t = total(rows);
  console.log('TOTAL'.padEnd(58), '', String(t.req).padStart(6), String(t.rustHits).padStart(8), fmtRate(t.rustHits, t.req), String(t.writeErr).padStart(6));
  return t;
}

function printDiff(a, b) {
  console.log('\nDELTA (second run - first)');
  console.log('job'.padEnd(58), 'wall_s', 'req'.padStart(6), 'rustHit'.padStart(8), 'rate.pp'.padStart(7), 'wErr'.padStart(6));
  for (const rb of b) {
    const ra = a.find((x) => x.name === rb.name);
    const dWall = ra && ra.wall != null && rb.wall != null ? rb.wall - ra.wall : null;
    const dReq = ra ? rb.req - ra.req : rb.req;
    const dHit = ra ? rb.rustHits - ra.rustHits : rb.rustHits;
    const dRate = ra && ra.req ? (100 * rb.rustHits / rb.req) - (100 * ra.rustHits / ra.req) : null;
    const dErr = ra ? rb.writeErr - ra.writeErr : rb.writeErr;
    console.log(rb.name.slice(0, 57).padEnd(58), dWall == null ? '    -' : String(dWall).padStart(5),
      String(dReq).padStart(6), String(dHit).padStart(8), dRate == null ? '    -' : dRate.toFixed(1).padStart(6),
      String(dErr).padStart(6));
  }
}

const [runId, baseId] = process.argv.slice(2);
if (!runId) {
  console.error('usage: node ci-analyze.js <run-id> [<baseline-run-id>]');
  process.exit(1);
}
const rows = parseRun(runId);
const t1 = printRows(`run ${runId}`, rows);
if (baseId) {
  const base = parseRun(baseId);
  printRows(`\nbaseline run ${baseId}`, base);
  printDiff(base, rows);
}
console.log(`\nRust hit rate ${(100 * t1.rustHits / t1.req).toFixed(1)}% across ${t1.req} compile requests (${rows.length} compiling jobs).`);
