"""Prepare a fresh stopped HTTP recovery observation package; never activate it."""
import argparse
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))
from uds_volume import prepare_uds
from config_bounds import bind_config

RUN = 'copybot-run15-http-recovery-probe-07'


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    path.chmod(0o600)


def prepare(previous, source_run, package, baseline, repository):
    if package.exists():
        raise ValueError('new_unused_package_required')
    os.umask(0o077)
    package.mkdir(mode=0o700)
    for name in ['config', 'control', 'state', 'install', 'evidence', 'ca', 'scripts']:
        (package / name).mkdir(mode=0o700)
    (package / 'install/state').mkdir(mode=0o700)  # Nested state mount under readonly install.
    for name in ['STOP', 'STREAM_STOP', 'HTTP_STOP']:
        (package / 'control' / name).write_text('inactive preparation; owner permission pending\n')
    for name in ['hosts', 'resolv.conf', 'settings.json']:
        shutil.copy2(previous / 'control' / name, package / 'control' / name)
    settings = json.loads((package / 'control/settings.json').read_text())
    settings['run_id'] = RUN
    save(package / 'control/settings.json', settings)
    shutil.copy2(previous / 'ca/public-roots.pem', package / 'ca/public-roots.pem')
    shutil.copy2(previous / 'config/read-only.toml', package / 'config/read-only.toml')
    config = package / 'config/read-only.toml'
    config.write_text(bind_config(config.read_text()))
    scope = json.loads((previous / 'SCOPE.json').read_text())
    scope.update(run_id=RUN, status='STOPPED_OWNER_DECISION_PENDING',
                 additional_rpc_cu=40960, http_rpc_requests=1024, rpc_cu_cap=40960,
                 http_model_usd_cap=0.021504, permission='OWNER_DECISION_PENDING')
    save(package / 'SCOPE.json', scope)
    history = json.loads(baseline.read_text())
    save(package / 'CARRYOVER.json', history)
    old_policy = json.loads((source_run / 'control/policy.json').read_text())
    policy = {key: value for key, value in old_policy.items() if key.startswith('rpc_')}
    policy.update(run_id=RUN, profile='read_only_http_recovery_v1', max_rpc_attempts=1024,
                  max_rpc_cu=40960,
                  prior_http_nano_usd=int(Decimal(str(history['http_model_usd'])) * 10**9),
                  prior_model_nano_usd=int(Decimal(str(history['cumulative_model_usd'])) * 10**9),
                  stream_reserved_nano_usd=400_000_000,
                  generation=1)
    save(package / 'control/http-policy.json', policy)
    shutil.copytree(repository / 'tools/http_recovery_broker', package / 'scripts/http_broker',
                    ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
    shutil.copytree(repository / 'tools/forward_runner/helpers', package / 'scripts/helpers',
                    ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
    shutil.copy2(repository / 'tools/transport_probe/probe_relay.py', package / 'scripts/probe_relay.py')
    save(package / 'PREPARATION.json', {'run_id': RUN, 'status': 'STOPPED_ARTIFACT_PENDING',
         'financial_authority': False, 'signer': False, 'new_database': 'not created',
         'old_packages': 'preserved; state is not copied or reset',
         'duration_seconds': 480, 'stream_byte_cap': 4 * 1024**3,
         'upstream_attempt_cap': 3, 'rpc_attempt_cap': 1024, 'rpc_cu_cap': 40960,
         'additional_model_usd_cap': 0.421504, 'new_paid_actions': 0})
    prepare_uds(RUN, package / 'evidence')
    shutil.copy2(repository / 'tools/http_recovery_probe/uds_volume.py', package / 'scripts/uds_volume.py')
    shutil.copy2(repository / 'tools/http_recovery_probe/config_bounds.py', package / 'scripts/config_bounds.py')
    shutil.copy2(repository / 'tools/http_recovery_probe/runtime_resources.py', package / 'scripts/runtime_resources.py')
    return package


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    for name in ['previous', 'source-run', 'package', 'baseline', 'repository']:
        p.add_argument('--' + name, required=True, type=Path)
    a = p.parse_args()
    print(json.dumps({'prepared': str(prepare(a.previous, a.source_run, a.package, a.baseline, a.repository)),
                      'activated': False}))
