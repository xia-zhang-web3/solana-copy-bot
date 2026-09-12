const assert = require('node:assert/strict');
const path = require('node:path');
const { test } = require('node:test');
const { loadExecution } = require('./execution-render-loader.cjs');
const repo = path.resolve(__dirname, '../../..');

function render(rows) {
  const snapshot = { source: 'synthetic', generated_at: '2026-09-05T12:00:00Z', stale: false,
    data: rows === undefined ? undefined : { status: 'ok', rows } };
  const runtime = loadExecution(repo, snapshot);
  const html = runtime.ReactDOMServer.renderToStaticMarkup(runtime.React.createElement(runtime.Execution));
  assert.deepEqual(runtime.boundaryCalls, ['/api/execution']);
  assert.match(html, /<h1>Execution<\/h1>/);
  return html;
}
function cash(known, cohort = known, settled = '1', unsettled = '0') {
  return [['exit_confirmed', '0'], ['cash_settled_orders', settled],
    ['cash_unsettled_orders', unsettled], ['known_cash_result_lamports', known],
    ['cohort_cash_result_lamports', cohort], ['economic_pnl', 'unknown']];
}
function field(html, key, value) {
  assert.ok(html.includes(`data-cash-field="${key}">${value}</dd>`), `${key} must show exact ${value}`);
}
for (const value of ['-36', '36', '0', '9007199254740993', '-9007199254740993']) {
  test(`real Execution preserves cash ${value}`, () => {
    const html = render(cash(value));
    field(html, 'known_cash_result_lamports', value);
    field(html, 'cohort_cash_result_lamports', value);
    field(html, 'cash_settled_orders', '1');
    field(html, 'cash_unsettled_orders', '0');
    field(html, 'economic_pnl', 'Unknown');
    assert.match(html, /Shadow-linked sells confirmed/);
    assert.match(html, /including owned-only/);
    assert.match(html, /fees\/rent\/WSOL decomposition unresolved/);
  });
}
for (const [name, rows, known, settled, unsettled] of [
  ['mixed', cash('-36', 'unknown', '1', '2'), '-36', '1', '2'],
  ['empty cohort', cash('0', 'unknown', '0', '0'), '0', '0', '0'],
  ['legacy', [['exit_confirmed', '30']], 'Unknown', 'Unknown', 'Unknown'],
  ['missing cash subtree', [['cash_settled_orders', 'Not reported']], 'Unknown', 'Unknown', 'Unknown'],
  ['missing data', undefined, 'Unknown', 'Unknown', 'Unknown'],
  ['empty rows', [], 'Unknown', 'Unknown', 'Unknown'],
  ['null values', cash(null), 'Unknown', '1', '0'],
  ['numeric amounts rejected', cash(9007199254740992), 'Unknown', '1', '0'],
]) {
  test(`real Execution handles ${name}`, () => {
    const html = render(rows);
    field(html, 'known_cash_result_lamports', known);
    field(html, 'cohort_cash_result_lamports', 'Unknown');
    field(html, 'cash_settled_orders', settled);
    field(html, 'cash_unsettled_orders', unsettled);
    field(html, 'economic_pnl', 'Unknown');
  });
}
