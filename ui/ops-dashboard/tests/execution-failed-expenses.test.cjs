const assert = require('node:assert/strict');
const path = require('node:path');
const {test} = require('node:test');
const {loadExecution} = require('./execution-render-loader.cjs');
const repo = path.resolve(__dirname,'../../..');
function render(rows) {
  const snapshot={source:'synthetic',stale:false,generated_at:'2026-09-05T01:00:00Z',data:{rows}};
  const rt=loadExecution(repo,snapshot);
  const html=rt.ReactDOMServer.renderToStaticMarkup(rt.React.createElement(rt.Execution));
  assert.deepEqual(rt.boundaryCalls,['/api/execution']);
  assert.ok(rt.sourceManifest.some(v=>v.path.endsWith('ExecutionFailedExpensePanel.tsx')));
  assert.match(html,/Economics unresolved/);
  return html;
}
function field(html,key,value) {assert.ok(html.includes(`data-expense-field="${key}">${value}</dd>`),`${key}: ${value}`);}
function rows(fee,coverage='complete_selected_cohort',unknown='0',unresolved=unknown,total='1') {
  return [['failed_expense_known_lamports',fee],['failed_expense_cohort_lamports',fee],['failed_expense_coverage',coverage],['failed_expense_orders',total],['failed_expense_unknown_orders',unknown],['failed_expense_unresolved_orders',unresolved],['failed_expense_native_delta_lamports','-9007199254740993'],['failed_expense_unexplained_lamports','-1'],['failed_expense_since','2026-09-05T00:00:00Z'],['failed_expense_as_of','2026-09-05T01:00:00Z'],['failed_expense_history','unverified_prior_history_no_backfill']];
}
for (const fee of ['0','5000','9007199254740993','18446744073709551615','36893488147419103230']) {
  test(`real React failed fee stays exact ${fee}`,()=>{
    const html=render(rows(fee));field(html,'failed_expense_known_lamports',fee);field(html,'failed_expense_cohort_lamports',fee);
    field(html,'failed_expense_native_delta_lamports','-9007199254740993');field(html,'failed_expense_unexplained_lamports','-1');
    assert.match(html,/Economic PnL remains Unknown/);assert.match(html,/original submit time/);assert.match(html,/2026-09-05T00:00:00Z/);
  });
}
for (const [name,input,expected] of [
 ['mixed',rows('0','partial_unresolved','1'),'0'],['unexplained',rows('5','partial_unresolved','0','1'),'5'],
 ['empty',rows('0','empty_unknown','0','0','0'),'0'],['missing',[], 'Unknown'],
 ['numeric',rows(9007199254740992),'Unknown'],['noncanonical',rows('01'),'Unknown'],['null',rows(null),'Unknown']
]) {test(`real React failed fees ${name} cannot be economic green`,()=>{const html=render(input);field(html,'failed_expense_known_lamports',expected);field(html,'failed_expense_cohort_lamports','Unknown');});}

test('real exporter JSON reaches the real React Execution component',()=>{
  const fs=require('node:fs');const os=require('node:os');const {execFileSync}=require('node:child_process');
  const binary=process.env.COPYBOT_DASHBOARD_EXPORT_BIN;
  assert.ok(binary,'test requires the freshly built exporter binary');
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'failed-ui-boundary-'));
  try {
    fs.mkdirSync(path.join(dir,'in'));
    fs.writeFileSync(path.join(dir,'in/execution_canary_quote_pnl.json'),JSON.stringify({as_of:new Date().toISOString(),tiny_execution_proof:{failed_expenses:{
      known_wallet_fee_lamports:'9007199254740993',cohort_wallet_fee_lamports:null,total_orders:2,unknown_orders:1,unresolved_orders:1,legacy_uncovered_orders:1,
      known_native_delta_lamports:'-9007199254740994',known_unexplained_delta_lamports:'-1',coverage:'partial_unresolved',source_basis:'failed_getTransaction_confirmed_or_finalized',history_coverage:'unverified_prior_history_no_backfill'
    }}}));
    execFileSync(binary,['--input-dir',path.join(dir,'in'),'--output-dir',path.join(dir,'out')]);
    const payload=JSON.parse(fs.readFileSync(path.join(dir,'out/execution.json'),'utf8'));
    const html=render(payload.data.rows);field(html,'failed_expense_known_lamports','9007199254740993');field(html,'failed_expense_cohort_lamports','Unknown');field(html,'failed_expense_unknown_orders','1');field(html,'failed_expense_unexplained_lamports','-1');
  } finally {fs.rmSync(dir,{recursive:true,force:true});}
});
