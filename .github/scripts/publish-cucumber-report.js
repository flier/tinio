module.exports = async ({ github, context, core }) => {
  const fs = require('fs');
  const path = require('path');

  // Find the cucumber JSON reports (same files the artifact
  // step uploads: *-report*.json anywhere in the workspace).
  function findReports(dir, acc) {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      if (entry.name === '.git' || entry.name === 'target') continue;
      const p = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        findReports(p, acc);
      } else if (/^.*-report.*\.json$/.test(entry.name)) {
        acc.push(p);
      }
    }
    return acc;
  }

  const reports = findReports(process.env.GITHUB_WORKSPACE, []).sort();
  const rows = [];
  const failures = [];

  for (const file of reports) {
    let data;
    try {
      data = JSON.parse(fs.readFileSync(file, 'utf8'));
    } catch (err) {
      core.warning(`could not parse report ${file}: ${err.message}`);
      continue;
    }
    // A matched file that is valid JSON but not a cucumber report array
    // must skip with a warning, not throw — this step never fails the job.
    if (!Array.isArray(data)) {
      core.warning(`skipping ${file}: not a cucumber JSON array`);
      continue;
    }
    const row = { file: path.basename(file), scenarios: 0, passed: 0, failed: 0, skipped: 0 };
    for (const feature of data) {
      for (const el of feature.elements || []) {
        if (el.type !== 'scenario') continue;
        row.scenarios += 1;
        // cucumber-rs 0.23's JSON writer accumulates events across
        // retry attempts on the same element (steps are never
        // cleared between attempts; element identity is
        // name+line+type), so a flake recovered by --retry 2
        // carries failed attempt-N statuses AND passed attempt-N+1
        // statuses for the same step lines. Keep only the LAST
        // status per step line (the gherkin position line is
        // stable across attempts); hooks have no stable key in
        // the JSON, so keep the last status per hook kind.
        const statuses = new Set();
        const lastByLine = new Map();
        for (const step of el.steps || []) {
          const status = step.result && step.result.status;
          if (typeof step.line === 'number') {
            lastByLine.set(step.line, status);
          } else {
            statuses.add(status);
          }
        }
        for (const status of lastByLine.values()) {
          statuses.add(status);
        }
        const lastHookByKind = new Map();
        for (const kind of ['before', 'after']) {
          for (const hook of el[kind] || []) {
            lastHookByKind.set(kind, hook.result && hook.result.status);
          }
        }
        for (const status of lastHookByKind.values()) {
          statuses.add(status);
        }
        if (statuses.has('failed') || statuses.has('undefined') || statuses.has('ambiguous')) {
          row.failed += 1;
          failures.push(`- \`${feature.uri}\` — ${el.name}`);
        } else if (statuses.size === 0 || (statuses.size === 1 && statuses.has('passed'))) {
          row.passed += 1;
        } else {
          row.skipped += 1;
        }
      }
    }
    rows.push(row);
  }

  const table = rows.length > 0
    ? ['| report | scenarios | passed | failed | skipped |', '|---|---|---|---|---|']
      .concat(rows.map((r) => `| ${r.file} | ${r.scenarios} | ${r.passed} | ${r.failed} | ${r.skipped} |`))
      .join('\n')
    : 'No cucumber report found — the e2e run did not produce a JSON report.';

  let note = '';
  if (failures.length > 0) {
    note = `\n**${failures.length} failed scenario(s):**\n${failures.join('\n')}`;
  } else if (rows.length > 0) {
    note = '\nAll scenarios passed.';
  }
  const body = [
    `## cucumber report (\`${process.env.GITHUB_JOB}\`)`,
    '',
    table,
    note,
  ].join('\n');

  // Step summary: visible on every run, including fork PRs
  // without comment permissions.
  await core.summary.addRaw(body).write();

  // PR comment (pull_request runs only — context.issue.number is
  // unset on push). A permission failure must not fail the job.
  if (context.issue.number) {
    try {
      await github.rest.issues.createComment({
        ...context.repo,
        issue_number: context.issue.number,
        body,
      });
    } catch (err) {
      core.warning(`could not post PR comment: ${err.message}`);
    }
  }
};
