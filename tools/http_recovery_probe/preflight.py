"""Read-only inspection of the stopped package. No Docker starts or providers."""
import argparse
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tomllib

RUN = 'copybot-run15-http-recovery-probe-05'
ROLES = {'observation-app', 'stream-front', 'stream-backend', 'http-front', 'http-backend'}
LABEL = 'copybot.http-recovery-probe'
PY_IMAGE = 'sha256:09ecaa87c6799c8d8ee0dfb779905d97e5f667ab97d866cc47ff9f949ffb7b3b'
APP_IMAGE = 'docker.io/library/ubuntu@sha256:224a1869083a311ef3f13648a154ba79832fbef6364d31493642ca03082da254'
APP_COMMAND = ['-i', 'SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt',
               '/opt/copybot/bin/copybot-app', '--config', '/run/probe-config/read-only.toml']
CA_FILE = '/etc/ssl/certs/ca-certificates.crt'
BACKEND_COMMAND = ['-B', '/code/http_broker/broker.py', 'backend', '/control/http-policy.json']
FLAGS = ['enabled', 'canary_tiny_submit_enabled', 'canary_enabled', 'canary_entry_submit_enabled',
         'quote_canary_enabled', 'swap_instructions_dry_run_enabled', 'swap_transaction_dry_run_enabled',
         'entry_quote_shadow_diagnostic_enabled', 'exit_policy_shadow_quote_enabled',
         'market_exit_shadow_quote_enabled', 'priority_fee_canary_enabled', 'simulate_before_submit',
         'quote_canary_public_parallel_enabled', 'quote_canary_pump_fun_parallel_enabled']
AUTHORITIES = ['technical_cohort', 'native_fresh_buy', 'owned_sell_preparation', 'owner_technical_buy', 'owner_exit']


def require(value, reason):
    if not value:
        raise ValueError(reason)


def read(path):
    return json.loads(Path(path).read_text())


def digest(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def host_mount_path(source):
    require(isinstance(source, str) and Path(source).is_absolute(), 'probe_absolute_mount_required')
    # Docker Desktop exposes some macOS binds through its Linux VM prefix.
    if sys.platform == 'darwin' and source.startswith('/host_mnt/'):
        source = source[len('/host_mnt'):]
    return Path(source).resolve()


def unused(root):
    for name in ['STOP', 'HTTP_STOP', 'STREAM_STOP']:
        require((root / 'control' / name).is_file(), 'probe_stop_missing:' + name)
    for directory in [root, root / 'control', root / 'evidence']:
        for name in ['ATTEMPT.json', 'PROBE_CLOCK.json', 'LEASE.json', 'RESULT.json', 'LIVE_RESULT.json']:
            require(not (directory / name).exists(), 'probe_consumed:' + name)
    require(not any((root / 'state').iterdir()), 'probe_state_already_used')
    nested = root / 'install/state'
    require(nested.is_dir() and not any(nested.iterdir()), 'probe_nested_state_mountpoint_missing_or_used')
    for path in root.rglob('*'):
        if not path.is_file():
            continue
        name = path.name.lower()
        require(not (re.search(r'\.(db|sqlite|sqlite3)(-wal|-shm)?$', name)
                     or name in ['authority.json', 'keypair.json', 'signer.json', 'ledger.json']),
                'probe_financial_or_runtime_material_present')


def configuration(root):
    c = tomllib.loads((root / 'config/read-only.toml').read_text())
    e, i = c['execution'], c['ingestion']
    require(all(e.get(k, False) is False for k in FLAGS), 'probe_financial_flag')
    require(not any(k in e for k in AUTHORITIES), 'probe_authority_config')
    require(not e.get('tiny_experiment', {}).get('activate', False), 'probe_tiny_activation')
    require(not e['execution_signer_keypair_path'] and not e['execution_signer_pubkey'], 'probe_signer_config')
    require(c['shadow']['enabled'] is False, 'probe_shadow_enabled')
    require(i['source'] == 'yellowstone_grpc' and i['yellowstone_delivery_mode'] == 'durable_association_v1',
            'probe_delivery_mode')
    h = i['yellowstone_http_recovery']
    require(h['broker_url'] == 'http://127.0.0.1:18765/rpc' and h['broker_token'] == '', 'probe_broker_binding')
    require((h['range_slots'], h['max_response_bytes'], h['timeout_ms'], h['fetch_concurrency'])
            == (1024, 8_388_608, 15_000, 4), 'probe_http_bounds')
    require(h['fetch_concurrency'] <= i['fetch_concurrency'], 'probe_http_concurrency')
    for name in ['pending', 'blocks', 'history', 'outputs', 'queue', 'inbox']:
        b = i['yellowstone_association'][name]
        require(type(b['count']) is int and b['count'] > 0 and type(b['bytes']) is int and b['bytes'] > 0,
                'probe_ingress_bounds')
    return c


def budgets(root, config):
    s, p, carry = [read(root / name) for name in ['SCOPE.json', 'control/http-policy.json', 'CARRYOVER.json']]
    require(s['run_id'] == p['run_id'] == RUN, 'probe_run_identity')
    require(s['permission'] == 'OWNER_DECISION_PENDING' and s['provider_permission'] == 'SEPARATE_OWNER_DECISION_REQUIRED',
            'probe_owner_permission_not_pending')
    require(s['financial_activation'] is False and s['financial_submissions'] == s['signatures'] == 0, 'probe_financial_scope')
    require((s['duration_seconds'], s['stream_byte_cap'], s['upstream_attempt_cap'], s['automatic_app_restart_cap'])
            == (480, 4 * 1024**3, 3, 1), 'probe_stream_caps')
    require((s['http_rpc_requests'], s['rpc_cu_cap'], s['additional_rpc_cu']) == (1024, 40960, 40960), 'probe_rpc_caps')
    require(Decimal(str(s['http_model_usd_cap'])) == Decimal('0.021504')
            and Decimal(str(s['stream_model_usd_cap'])) == Decimal('0.4')
            and s['cumulative_budget_usd_cap'] == 50, 'probe_model_caps')
    require(p['profile'] == 'read_only_http_recovery_v1' and p['generation'] == 1
            and p['max_rpc_attempts'] == 1024 and p['max_rpc_cu'] == 40960
            and not p.get('offline_stub'), 'probe_broker_caps')
    require(p['prior_http_nano_usd'] == int(Decimal(str(carry['http_model_usd'])) * 10**9)
            and p['prior_model_nano_usd'] == int(Decimal(str(carry['cumulative_model_usd'])) * 10**9)
            and p['stream_reserved_nano_usd'] == 400_000_000, 'probe_carryover_model')
    require(Decimal(str(carry['cumulative_model_usd'])) + Decimal('0.421504') <= 50, 'probe_cumulative_cap')
    require(config['ingestion']['yellowstone_replay_wallets'] == s['wallets']
            and 1 <= len(s['wallets']) <= 4 and len(set(s['wallets'])) == len(s['wallets']), 'probe_wallet_scope')
    settings = read(root / 'control/settings.json')
    require(settings['run_id'] == RUN and settings['max_connections'] == 3, 'probe_relay_cap')
    for path, expected in carry['history_files_sha256'].items():
        require(digest(path) == expected, 'probe_history_changed')
    return p


def artifact(root, expected_sha):
    require(isinstance(expected_sha, str) and re.fullmatch('[0-9a-f]{40}', expected_sha), 'probe_explicit_accepted_sha_required')
    binding = read(root / 'install/ARTIFACT_BINDING.json')
    require(binding['status'] == 'ACCEPTED_MATCHING_CI_ARTIFACT'
            and binding['git_sha'] == binding['expected_git_sha'] == expected_sha, 'probe_artifact_identity')
    binary = root / 'install/bin/copybot-app'
    require(binary.resolve().is_relative_to((root / 'install').resolve())
            and digest(binary) == binding['binary_sha256'], 'probe_binary_checksum')
    manifest = read(root / 'install/bin/packages/copybot-app/current/build-manifest.json')
    require(manifest['git_sha'] == expected_sha and manifest['package'] == 'copybot-app'
            and manifest['profile'] == 'release', 'probe_artifact_manifest')
    directory = root / 'install/migrations'
    hashes = binding['migration_files_sha256']
    require({str(p.relative_to(directory)) for p in directory.rglob('*.sql')} == set(hashes), 'probe_migration_set')
    for name, expected in hashes.items():
        path = directory / name
        require(path.resolve().is_relative_to(directory.resolve()) and digest(path) == expected, 'probe_migration_checksum')
    return binding


def docker_inspect(cid):
    require(re.fullmatch('[0-9a-f]{64}', cid), 'probe_container_id')
    result = subprocess.run(['docker', 'inspect', cid], capture_output=True, text=True, timeout=30)
    require(result.returncode == 0, 'probe_docker_inspect_failed')
    rows = json.loads(result.stdout)
    require(len(rows) == 1, 'probe_container_inspect_count')
    return rows[0]


def containers(root, inspect, policy):
    ids = read(root / 'CONTAINERS.json')
    require(set(ids) == ROLES and len(set(ids.values())) == 5, 'probe_five_container_roles')
    uds = read(root / 'evidence/UDS_VOLUME_BINDING.json')
    require(uds['status'] == 'PASS_FRESH_OWNED_UDS' and uds['run_id'] == RUN
            and uds['volume'] == RUN + '-uds' and uds['image'] == PY_IMAGE
            and uds['labels'] == {LABEL: RUN, 'copybot.resource-role': 'uds'}
            and uds['ownership']['after'] == dict(uid=501, gid=20, mode='0o700')
            and uds['unix_bind'] == dict(unix_listener_bind='PASS', uid=501, gid=20,
                                        network='none', provider_calls=0)
            and uds['provider_roles_created'] == uds['provider_calls'] == uds['signatures'] == uds['submissions'] == 0,
            'probe_uds_owner_or_bind_proof')
    for role, cid in ids.items():
        require(re.fullmatch('[0-9a-f]{64}', cid), 'probe_container_id')
        value = inspect(cid)
        labels, state, host = value['Config']['Labels'], value['State'], value['HostConfig']
        require(value['Config']['Image'] == (APP_IMAGE if role == 'observation-app' else PY_IMAGE)
                and value.get('Platform') == 'linux', 'probe_cached_image_identity')
        if role == 'observation-app':
            require(value['Config'].get('Entrypoint') == ['/usr/bin/env']
                    and value['Config'].get('Cmd') == APP_COMMAND, 'probe_app_config_command')
            require(value['Config'].get('WorkingDir') == '/opt/copybot', 'probe_app_working_directory')
        if role == 'http-backend':
            config = value['Config']
            require(config.get('Entrypoint') == ['/usr/bin/python3']
                    and config.get('Cmd') == BACKEND_COMMAND, 'probe_backend_command')
            require([item for item in config.get('Env', []) if item.startswith('SSL_CERT_FILE=')]
                    == ['SSL_CERT_FILE=' + CA_FILE]
                    and not any(item.startswith('BROKER_OFFLINE_TEST=') for item in config.get('Env', [])),
                    'probe_backend_ca_environment')
            ca = [mount for mount in value['Mounts'] if mount['Destination'] == CA_FILE]
            require(len(ca) == 1 and ca[0]['Type'] == 'bind' and ca[0]['RW'] is False
                    and host_mount_path(ca[0]['Source']) == (root / 'ca/public-roots.pem').resolve(),
                    'probe_backend_ca_mount')
            proof = read(root / 'evidence/HTTP_BACKEND_TLS_BINDING.json')
            require(proof['image'] == PY_IMAGE and proof['ca_sha256'] == digest(root / 'ca/public-roots.pem')
                    and proof['ssl_cert_file'] == CA_FILE and proof['verify_mode'] == 'CERT_REQUIRED'
                    and proof['check_hostname'] is True and type(proof['ca_certificates']) is int
                    and proof['ca_certificates'] > 0, 'probe_backend_effective_ca_context')
        if role == 'http-front':
            control = [mount for mount in value['Mounts'] if mount['Destination'] == '/control']
            require(len(control) == 1 and control[0]['Type'] == 'bind' and control[0]['RW'] is True
                    and host_mount_path(control[0]['Source']) == (root / 'control').resolve(),
                    'probe_front_clock_and_delivery_archive_mount')
        require(value['Id'] == cid and value['Name'] == '/' + RUN + '-' + role
                and labels.get(LABEL) == RUN and labels.get('copybot.role') == role, 'probe_container_ownership')
        require(state['Status'] == 'created' and not state['Running'] and not state.get('OOMKilled', False)
                and state['StartedAt'] == '0001-01-01T00:00:00Z' and value['RestartCount'] == 0, 'probe_container_started')
        require(host['ReadonlyRootfs'] and host['RestartPolicy']['Name'] == 'no'
                and 'ALL' in host['CapDrop'] and 'no-new-privileges' in host['SecurityOpt']
                and value['Config']['User'] == '501:20', 'probe_container_lifecycle')
        network = ('container:' + ids['stream-front'] if role in ['observation-app', 'http-front']
                   else 'none' if role == 'stream-front' else 'bridge')
        require(host['NetworkMode'] == network and not host['PortBindings'], 'probe_container_network')
        for mount in value['Mounts']:
            target = mount['Destination'].lower()
            require(not any(word in target for word in ['signer', 'keypair', 'authority']), 'probe_financial_mount')
            if mount['Type'] == 'volume':
                require(mount['Name'] == uds['volume'] and target.startswith('/relay'), 'probe_volume_scope')
            elif target == '/run/provider/alchemy-api-key':
                require(role == 'http-backend' and mount['RW'] is False
                        and digest(host_mount_path(mount['Source'])) == policy['rpc_key_sha256'], 'probe_provider_mount')
            else:
                require(mount['Type'] == 'bind' and host_mount_path(mount['Source']).is_relative_to(root.resolve()),
                        'probe_foreign_mount')
                if mount['RW']:
                    require(target == '/control' or (role == 'observation-app' and target == '/opt/copybot/state'),
                            'probe_writable_mount')
    return len(ids)


def preflight(root, expected_sha=None, inspect=None, require_artifact=True, require_containers=True):
    root = Path(root)
    unused(root)
    config = configuration(root)
    policy = budgets(root, config)
    binding = artifact(root, expected_sha) if require_artifact else None
    count = containers(root, inspect or docker_inspect, policy) if require_containers else 0
    return dict(status='PASS_STOPPED_OWNER_DECISION_PENDING' if binding and count == 5 else 'PREPARATION_CHECK_ONLY',
                run_id=RUN, git_sha=binding['git_sha'] if binding else None, never_started_containers=count,
                stops=3, financial_activation=False, new_provider_calls=0, signatures=0, submissions=0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--package', type=Path, required=True)
    parser.add_argument('--expected-sha', required=True)
    args = parser.parse_args()
    print(json.dumps(preflight(args.package, args.expected_sha), sort_keys=True))
