"""Generate a sealed offline config, never retaining provider credential strings."""
import json
from pathlib import Path
import re
import tomllib


def prepare(source, target, grpc_url, broker_url, wallet, blocks_bytes=805306368):
    assert blocks_bytes in (805306368, 1744830464), 'bounded offline block cache choices'
    text = Path(source).read_text()
    original = tomllib.loads(text)
    section = ''
    output = []
    for line in text.splitlines():
        match = re.match(r'^\[([^\]]+)\]', line)
        if match:
            section = match[1]
        assignment = re.match(r'^(\w+)\s*=\s*(.*)$', line)
        if assignment:
            key, value = assignment.groups()
            substitute = None
            node = original
            for component in section.split('.') if section else []:
                node = node[component]
            parsed = node[key]
            if isinstance(parsed, str) and (key.endswith('_url') or 'endpoint' in key):
                substitute = json.dumps('http://127.0.0.1:1')
            elif key.endswith('_token') and isinstance(parsed, str):
                substitute = json.dumps('offline-no-provider')
            elif isinstance(parsed, str) and (key.endswith('_keypair_path') or key.endswith('_key') or key.endswith('_api_key')):
                substitute = json.dumps('')
            if (section, key) == ('sqlite', 'path'):
                substitute = json.dumps('/opt/copybot/state/live_runtime.db')
            elif (section, key) == ('ingestion', 'yellowstone_grpc_url'):
                substitute = json.dumps(grpc_url)
            elif (section, key) == ('ingestion', 'yellowstone_replay_wallets'):
                substitute = json.dumps([wallet])
            elif (section, key) == ('ingestion.yellowstone_http_recovery', 'broker_url'):
                substitute = json.dumps(broker_url)
            elif (section, key) == ('ingestion.yellowstone_http_recovery', 'broker_token'):
                substitute = json.dumps('')
            elif (section, key) == ('execution', 'canary_kill_switch_path'):
                substitute = json.dumps('/control/STOP')
            elif (section, key) == ('ingestion.yellowstone_http_recovery', 'timeout_ms'):
                substitute = '30000'
            elif (section, key) == ('ingestion.yellowstone_association', 'blocks'):
                assert parsed == {'count': 384, 'bytes': 805306368}, 'saved07 block cache binding'
                substitute = '{ count = 384, bytes = '+str(blocks_bytes)+' }'
            if substitute is not None:
                line = key+' = '+substitute
        output.append(line)
    raw = '\n'.join(output)+'\n'
    value = tomllib.loads(raw)
    for section in ['execution', 'shadow']:
        assert not value[section]['enabled']
    for key in ['canary_tiny_submit_enabled', 'canary_enabled', 'quote_canary_enabled']:
        assert not value['execution'][key]
    assert not value['execution']['execution_signer_keypair_path']
    assert value['ingestion']['yellowstone_association']['blocks']['bytes'] == blocks_bytes
    def validate(node):
        if isinstance(node, dict):
            for item in node.values(): validate(item)
        elif isinstance(node, list):
            for item in node: validate(item)
        elif isinstance(node, str):
            if 'https://' in node or 'http://' in node:
                assert node.startswith(('http://127.0.0.1:', 'http://host.docker.internal:'))
    validate(value)
    Path(target).write_text(raw)
    return value
