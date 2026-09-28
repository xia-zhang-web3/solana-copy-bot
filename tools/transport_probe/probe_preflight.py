"""Local inspection only: no Docker starts, provider requests or financial authority."""
import tomllib
from probe_common import *
from probe_docker import commands


def configuration():
    config = tomllib.loads((ROOT / 'config/read-only.toml').read_text())
    execution = config['execution']
    for key in ['enabled', 'canary_tiny_submit_enabled', 'canary_enabled', 'canary_entry_submit_enabled',
                'quote_canary_enabled', 'swap_instructions_dry_run_enabled', 'swap_transaction_dry_run_enabled',
                'entry_quote_shadow_diagnostic_enabled', 'exit_policy_shadow_quote_enabled',
                'market_exit_shadow_quote_enabled', 'priority_fee_canary_enabled', 'simulate_before_submit',
                'quote_canary_public_parallel_enabled', 'quote_canary_pump_fun_parallel_enabled']:
        if execution.get(key, False) is not False:
            raise ValueError('probe_financial_flag:' + key)
    if any(key in execution for key in ['technical_cohort', 'native_fresh_buy',
                                       'owned_sell_preparation', 'owner_technical_buy', 'owner_exit']):
        raise ValueError('probe_authority_present')
    if execution.get('tiny_experiment', {}).get('activate', False):
        raise ValueError('probe_tiny_activation')
    if execution['execution_signer_keypair_path'] or execution['execution_signer_pubkey']:
        raise ValueError('probe_signer_config')
    if not (ROOT / 'control/STOP').is_file():
        raise ValueError('probe_permanent_financial_stop_missing')
    scope = read(ROOT / 'SCOPE.json')
    if config['ingestion']['yellowstone_replay_wallets'] != scope['wallets']:
        raise ValueError('probe_replay_scope_mismatch')
    if config['ingestion']['yellowstone_delivery_mode'] != 'durable_association_v1':
        raise ValueError('probe_delivery_mode')
    if config['shadow']['enabled']:
        raise ValueError('probe_shadow_enabled')
    return config


def artifact():
    binding = read(ROOT / 'install/ARTIFACT_BINDING.json')
    if not binding or binding.get('status') != 'ACCEPTED_MATCHING_CI_ARTIFACT':
        raise ValueError('probe_matching_artifact_pending')
    if binding['git_sha'] != binding['expected_git_sha'] or digest(ROOT / 'install/bin/copybot-app') != binding['binary_sha256']:
        raise ValueError('probe_artifact_identity')
    manifest = read(ROOT / 'install/bin/packages/copybot-app/current/build-manifest.json')
    if manifest['git_sha'] != binding['git_sha'] or manifest['package'] != 'copybot-app' or manifest['profile'] != 'release':
        raise ValueError('probe_artifact_manifest')
    hashes = binding['migration_files_sha256']
    actual = {str(p.relative_to(ROOT / 'install/migrations')) for p in (ROOT / 'install/migrations').rglob('*.sql')}
    if actual != set(hashes):
        raise ValueError('probe_migration_set')
    for name, expected in hashes.items():
        if digest(ROOT / 'install/migrations' / name) != expected:
            raise ValueError('probe_migration_digest')
    return binding


def preflight(require_artifact=True):
    unconsumed()
    configuration()
    policy = read(ROOT / 'SCOPE.json')
    if (policy['duration_seconds'], policy['stream_byte_cap'], policy['upstream_attempt_cap']) != (SECONDS, CAP, 3):
        raise ValueError('probe_caps')
    if policy['http_rpc_requests'] != 0 or policy['financial_submissions'] != 0:
        raise ValueError('probe_scope')
    if read(ROOT / 'control/settings.json')['max_connections'] != 3:
        raise ValueError('probe_relay_cap')
    for path, expected in read(ROOT / 'CARRYOVER.json')['history_files_sha256'].items():
        if digest(path) != expected:
            raise ValueError('probe_source_history_changed')
    # No paid call is made by image inspect.
    for ref in [PY_IMAGE, APP_IMAGE]:
        value = json.loads(docker(['image', 'inspect', '--platform', 'linux/amd64', ref]))[0]
        if value['Os'] != 'linux' or value['Architecture'] != 'amd64':
            raise ValueError('probe_cached_image_platform')
    binding = artifact() if require_artifact else None
    cids = read(ROOT / 'CONTAINERS.json', {})
    for role, cid in cids.items():
        value = verify_container(cid)
        if value['State']['Running'] or value['State']['StartedAt'] != '0001-01-01T00:00:00Z':
            raise ValueError('probe_preflight_started_container')
        host = value['HostConfig']
        if not host['ReadonlyRootfs'] or host['RestartPolicy']['Name'] != 'no':
            raise ValueError('probe_container_lifecycle')
        expected = 'container:' + cids['stream-front'] if role == 'observation-app' else 'none' if role == 'stream-front' else 'bridge'
        if host['NetworkMode'] != expected or host['PortBindings']:
            raise ValueError('probe_container_network')
        if any('signer' in m['Destination'] or 'keypair' in m['Destination'] for m in value['Mounts']):
            raise ValueError('probe_signer_mount')
    return dict(status='PASS' if binding else 'PREPARATION_ONLY_ARTIFACT_PENDING', run_id=RUN,
        git_sha=binding['git_sha'] if binding else None, containers_created=len(cids),
        starts=0, provider_calls=0, signatures=0, submissions=0)
