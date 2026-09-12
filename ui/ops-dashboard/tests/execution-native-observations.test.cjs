const assert=require('node:assert/strict');
const path=require('node:path');
const {test}=require('node:test');
const {loadExecution}=require('./execution-render-loader.cjs');
const {report}=require('./native-observation-fixture.cjs');
const repo=path.resolve(__dirname,'../../..');
function render(data) {
  const rt=loadExecution(repo,{source:'synthetic',stale:false,generated_at:'2026-09-05T01:00:00Z',data:{rows:[],native_observations:data}});
  const html=rt.ReactDOMServer.renderToStaticMarkup(rt.React.createElement(rt.Execution));
  assert.deepEqual(rt.boundaryCalls,['/api/execution']);
  assert.ok(rt.sourceManifest.some(s=>s.path.endsWith('ExecutionNativeObservationsPanel.tsx')));
  assert.match(html,/Economic PnL remains Unknown/);return html;
}
for (const amount of ['0','9007199254740993','18446744073709551615']) {
  test(`actual Execution native observations retain exact ${amount}`,()=>{
    const html=render(report(amount));assert.ok(html.includes(`data-native-value="native_post">${amount} · known · rpc_native_balance`));
    assert.match(html,/ForeignDestination/);assert.match(html,/ForeignOwner/);assert.match(html,/rpc_outer_inner_positions|RPC outer\/inner positions/);
    assert.match(html,/data-native-count="total_orders">2/);assert.match(html,/Display limited: yes/);
  });
}
for (const [name,data] of [['empty',{...report(),rows:[],total_orders:'0',coverage:'empty_unknown'}],['uncovered',null],['numeric',report(9007199254740992)],['noncanonical',report('01')],['overflow',report('18446744073709551616')],['negative_unsigned',report('-1')]]) {
  test(`actual Execution native coverage ${name}`,()=>{const html=render(data);if(['numeric','noncanonical','overflow','negative_unsigned'].includes(name)) assert.match(html,/data-native-value="native_post">Unknown/);else assert.match(html,/No account observations displayed/);});
}
test('native primary JSON through actual exporter into actual React',()=>{
  const fs=require('node:fs'),os=require('node:os'),{execFileSync}=require('node:child_process');
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'native-ui-'));const binary=process.env.COPYBOT_DASHBOARD_EXPORT_BIN;assert.ok(binary);
  try {
    fs.mkdirSync(path.join(dir,'in'));
    for (const amount of ['18446744073709551615',9007199254740992]) {
      fs.writeFileSync(path.join(dir,'in/execution_canary_quote_pnl.json'),JSON.stringify({as_of:new Date().toISOString(),tiny_execution_proof:{native_observations:report(amount)}}));
      execFileSync(binary,['--input-dir',path.join(dir,'in'),'--output-dir',path.join(dir,'out')]);
      const payload=JSON.parse(fs.readFileSync(path.join(dir,'out/execution.json'),'utf8'));
      const html=render(payload.data.native_observations);
      if(typeof amount==='string') assert.match(html,/data-native-value="native_post">18446744073709551615/);
      else {assert.equal(payload.data.native_observations.coverage,'uncovered');assert.match(html,/data-native-count="total_orders">Unknown/);}
    }
  } finally {fs.rmSync(dir,{recursive:true,force:true});}
});
