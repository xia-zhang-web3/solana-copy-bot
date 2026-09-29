"""Hermetic inspection controls; Docker metadata is supplied, never launched."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

SOURCE = Path(__file__).resolve().parents[1] / 'http_recovery_probe/preflight.py'
spec = importlib.util.spec_from_file_location('http_probe_preflight', SOURCE)
pf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pf)
SHA = 'a' * 40


def save(path, value):
    path.write_text(json.dumps(value))


class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / 'package'
        for name in ['control', 'config', 'state', 'evidence', 'install/migrations', 'install/state',
                     'install/bin/packages/copybot-app/current', 'ca']:
            (self.root / name).mkdir(parents=True, exist_ok=True)
        for name in ['STOP', 'HTTP_STOP', 'STREAM_STOP']:
            (self.root / 'control' / name).touch()
        self.config = '''[execution]
enabled=false
execution_signer_keypair_path=""
execution_signer_pubkey=""
[execution.tiny_experiment]
activate=false
[shadow]
enabled=false
[ingestion]
source="yellowstone_grpc"
yellowstone_delivery_mode="durable_association_v1"
yellowstone_replay_wallets=["11111111111111111111111111111111"]
fetch_concurrency=8
[ingestion.yellowstone_http_recovery]
broker_url="http://127.0.0.1:18765/rpc"
broker_token=""
range_slots=1024
max_response_bytes=8388608
timeout_ms=15000
fetch_concurrency=4
'''
        for name in ['pending', 'blocks', 'history', 'outputs', 'queue', 'inbox']:
            self.config += f'[ingestion.yellowstone_association.{name}]\ncount=32\nbytes=1048576\n'
        (self.root / 'config/read-only.toml').write_text(self.config)
        self.history = Path(self.tmp.name) / 'accepted-history.json'
        self.history.write_text('{"accepted":true,"spending_preserved":true}')
        self.key = Path(self.tmp.name) / 'synthetic-api-key'
        self.key.write_text('synthetic-offline-only')
        self.key.chmod(0o600)
        self.ca = self.root / 'ca/public-roots.pem'
        self.ca.write_text('synthetic offline CA identity; actual HTTPS is exercised separately')
        save(self.root / 'evidence/HTTP_BACKEND_TLS_BINDING.json', dict(image=pf.PY_IMAGE,
             ca_sha256=pf.digest(self.ca), ssl_cert_file=pf.CA_FILE, verify_mode='CERT_REQUIRED',
             check_hostname=True, ca_certificates=128))
        save(self.root / 'evidence/UDS_VOLUME_BINDING.json', dict(status='PASS_FRESH_OWNED_UDS',
             run_id=pf.RUN, volume=pf.RUN+'-uds', image=pf.PY_IMAGE,
             labels={pf.LABEL:pf.RUN, 'copybot.resource-role':'uds'},
             ownership={'after':dict(uid=501, gid=20, mode='0o700')},
             unix_bind=dict(unix_listener_bind='PASS', uid=501, gid=20, network='none', provider_calls=0),
             provider_roles_created=0, provider_calls=0, signatures=0, submissions=0))
        save(self.root / 'CARRYOVER.json', dict(cumulative_model_usd=7.928686412,
             http_model_usd=0.0099855, http_requests=996, rpc_cu=19020,
             cumulative_stream_bytes=85026403600, history_files_sha256={str(self.history):pf.digest(self.history)}))
        save(self.root / 'control/settings.json', dict(run_id=pf.RUN, max_connections=3))
        save(self.root / 'control/http-policy.json', dict(run_id=pf.RUN, profile='read_only_http_recovery_v1',
             generation=1, max_rpc_attempts=1024, max_rpc_cu=40960, prior_http_nano_usd=9985500,
             prior_model_nano_usd=7928686412, stream_reserved_nano_usd=400000000, rpc_key_sha256=pf.digest(self.key)))
        save(self.root / 'SCOPE.json', dict(run_id=pf.RUN, permission='OWNER_DECISION_PENDING',
             provider_permission='SEPARATE_OWNER_DECISION_REQUIRED', financial_activation=False,
             financial_submissions=0, signatures=0, duration_seconds=480, stream_byte_cap=4*1024**3,
             upstream_attempt_cap=3, automatic_app_restart_cap=1, http_rpc_requests=1024, rpc_cu_cap=40960,
             additional_rpc_cu=40960, http_model_usd_cap=0.021504, stream_model_usd_cap=0.4,
             cumulative_budget_usd_cap=50, wallets=['1'*32]))
        self.binary = self.root / 'install/bin/copybot-app'
        self.binary.write_bytes(b'hermetic fake accepted artifact')
        self.migration = self.root / 'install/migrations/0089_test.sql'
        self.migration.write_text('-- hermetic migration digest control\n')
        save(self.root / 'install/ARTIFACT_BINDING.json', dict(status='ACCEPTED_MATCHING_CI_ARTIFACT',
             git_sha=SHA, expected_git_sha=SHA, binary_sha256=pf.digest(self.binary),
             migration_files_sha256={self.migration.name:pf.digest(self.migration)}))
        save(self.root / 'install/bin/packages/copybot-app/current/build-manifest.json',
             dict(git_sha=SHA, package='copybot-app', profile='release'))
        self.ids = {role:format(n,'064x') for n,role in enumerate(sorted(pf.ROLES),1)}
        save(self.root / 'CONTAINERS.json', self.ids)
        self.metadata = {}
        for role,cid in self.ids.items():
            network = ('container:'+self.ids['stream-front'] if role in ['observation-app','http-front']
                       else 'none' if role=='stream-front' else 'bridge')
            mounts=[dict(Type='volume', Name=pf.RUN+'-uds', Source='/synthetic-volume', Destination='/relay', RW=True)]
            if role=='http-front':
                mounts.append(dict(Type='bind', Source=str(self.root/'control'), Destination='/control', RW=True))
            if role=='http-backend':
                mounts.append(dict(Type='bind', Source=str(self.key), Destination='/run/provider/alchemy-api-key', RW=False))
                mounts.append(dict(Type='bind', Source=str(self.ca), Destination=pf.CA_FILE, RW=False))
            self.metadata[cid] = dict(Id=cid, Name='/'+pf.RUN+'-'+role, Platform='linux', RestartCount=0,
                Config=dict(Image=pf.APP_IMAGE if role=='observation-app' else pf.PY_IMAGE, User='501:20',
                    WorkingDir='/opt/copybot' if role=='observation-app' else '',
                    Entrypoint=['/usr/bin/python3'] if role=='http-backend' else ['/usr/bin/env'],
                    Cmd=list(pf.BACKEND_COMMAND if role=='http-backend' else pf.APP_COMMAND),
                    Env=['SSL_CERT_FILE='+pf.CA_FILE] if role=='http-backend' else [],
                    Labels={pf.LABEL:pf.RUN,'copybot.role':role}),
                State=dict(Status='created',Running=False,StartedAt='0001-01-01T00:00:00Z',OOMKilled=False),
                HostConfig=dict(NetworkMode=network,PortBindings={},ReadonlyRootfs=True,
                    RestartPolicy={'Name':'no'},CapDrop=['ALL'],SecurityOpt=['no-new-privileges']),Mounts=mounts)

    def tearDown(self):
        self.tmp.cleanup()

    def check(self, **kwargs):
        return pf.preflight(self.root, SHA, inspect=lambda cid:self.metadata[cid], **kwargs)

    def test_stopped_matching_five_container_package_and_preparation_only_are_distinct(self):
        result=self.check()
        self.assertEqual(result['status'],'PASS_STOPPED_OWNER_DECISION_PENDING')
        self.assertEqual((result['never_started_containers'],result['new_provider_calls']),(5,0))
        result=self.check(require_artifact=False,require_containers=False)
        self.assertEqual(result['status'],'PREPARATION_CHECK_ONLY')
        self.assertFalse(result['financial_activation'])

    def test_consumed_clock_database_and_missing_stop_are_rejected(self):
        for directory,name in [('control','PROBE_CLOCK.json'),('control','LEASE.json'),
                               ('control','ATTEMPT.json'),('evidence','RESULT.json'),('state','live_runtime.db')]:
            path=self.root/directory/name;path.write_text('{}')
            with self.assertRaises(ValueError):self.check()
            path.unlink()
        path=self.root/'control/HTTP_STOP';path.unlink()
        with self.assertRaisesRegex(ValueError,'probe_stop_missing'):self.check()

    def test_financial_activation_and_signer_configuration_are_rejected(self):
        path=self.root/'config/read-only.toml'
        for text in [self.config.replace('enabled=false','enabled=true',1),
                     self.config.replace('execution_signer_keypair_path=""','execution_signer_keypair_path="keypair.json"'),
                     self.config.replace('activate=false','activate=true',1)]:
            path.write_text(text)
            with self.assertRaises(ValueError):self.check()
        path.write_text(self.config)

    def test_stale_artifact_binary_migration_and_history_digests_are_rejected(self):
        with self.assertRaisesRegex(ValueError,'probe_artifact_identity'):
            pf.preflight(self.root,'b'*40,inspect=lambda cid:self.metadata[cid])
        for path in [self.binary,self.migration,self.history]:
            original=path.read_bytes();path.write_bytes(original+b'changed')
            with self.assertRaises(ValueError):self.check()
            path.write_bytes(original)

    def test_started_egress_owner_mount_and_lowered_carryover_are_rejected(self):
        cid=self.ids['observation-app'];original=json.dumps(self.metadata[cid])
        for mutate in [lambda v:v['State'].update(StartedAt='2026-09-28T00:00:00Z'),
                       lambda v:v['HostConfig'].update(NetworkMode='bridge'),
                       lambda v:v['Config']['Labels'].update({pf.LABEL:'another-run'}),
                       lambda v:v['Mounts'].append(dict(Type='bind',Source=str(self.key),Destination='/run/signer',RW=False))]:
            mutate(self.metadata[cid])
            with self.assertRaises(ValueError):self.check()
            self.metadata[cid]=json.loads(original)
        path=self.root/'control/http-policy.json';policy=pf.read(path)
        policy['prior_model_nano_usd']=0;save(path,policy)
        with self.assertRaisesRegex(ValueError,'probe_carryover_model'):self.check()

    def test_http_front_requires_its_owned_clock_and_delivery_archive_mount(self):
        cid=self.ids['http-front']
        control=next(m for m in self.metadata[cid]['Mounts'] if m['Destination']=='/control')
        for field,value in [('RW',False),('Source',str(self.root/'state'))]:
            original=control[field];control[field]=value
            with self.assertRaisesRegex(ValueError,'probe_front_clock_and_delivery_archive_mount'):
                self.check()
            control[field]=original
        self.metadata[cid]['Mounts'].remove(control)
        with self.assertRaisesRegex(ValueError,'probe_front_clock_and_delivery_archive_mount'):
            self.check()

    def test_desktop_bind_translation_preserves_package_scope_and_provider_digest(self):
        cid = self.ids['http-backend']
        mounts = self.metadata[cid]['Mounts']
        provider = next(m for m in mounts if m['Destination'] == '/run/provider/alchemy-api-key')
        provider['Source'] = '/host_mnt' + str(self.key)
        ca = dict(Type='bind', Source='/host_mnt' + str(self.root / 'config'),
                  Destination='/run/probe-config', RW=False)
        mounts.append(ca)
        with mock.patch.object(pf.sys, 'platform', 'darwin'):
            self.assertEqual(self.check()['never_started_containers'], 5)
            ca['Source'] = '/host_mnt' + str(self.history)
            with self.assertRaisesRegex(ValueError, 'probe_foreign_mount'):
                self.check()
            ca['Source'] = '/host_mnt' + str(self.root / 'config')
            original = self.key.read_bytes()
            self.key.write_bytes(b'wrong offline key')
            with self.assertRaisesRegex(ValueError, 'probe_provider_mount'):
                self.check()
            self.key.write_bytes(original)
        with mock.patch.object(pf.sys, 'platform', 'linux'):
            self.assertEqual(pf.host_mount_path('/host_mnt/Users/example'), Path('/host_mnt/Users/example'))

    def test_actual_daemon_config_command_and_nested_state_mountpoint_are_required(self):
        config = self.metadata[self.ids['observation-app']]['Config']
        for command in [
            ['COPYBOT_CONFIG_PATH=/run/probe-config/read-only.toml', '/opt/copybot/bin/copybot-app'],
            ['/opt/copybot/bin/copybot-app'],
            ['-i', '/opt/copybot/bin/copybot-app', '--config', '/foreign.toml'],
        ]:
            config['Cmd'] = command
            with self.assertRaisesRegex(ValueError, 'probe_app_config_command'):
                self.check()
        config['Cmd'] = list(pf.APP_COMMAND)
        nested = self.root / 'install/state'
        nested.rmdir()
        with self.assertRaisesRegex(ValueError, 'probe_nested_state_mountpoint'):
            self.check()

    def test_actual_working_directory_and_effective_backend_ca_binding_are_required(self):
        app = self.metadata[self.ids['observation-app']]['Config']
        app['WorkingDir'] = '/'
        with self.assertRaisesRegex(ValueError, 'probe_app_working_directory'):
            self.check()
        app['WorkingDir'] = '/opt/copybot'
        backend = self.metadata[self.ids['http-backend']]['Config']
        for environment in [[], ['SSL_CERT_FILE=/wrong.pem'],
                            ['SSL_CERT_FILE='+pf.CA_FILE, 'SSL_CERT_FILE='+pf.CA_FILE]]:
            backend['Env'] = environment
            with self.assertRaisesRegex(ValueError, 'probe_backend_ca_environment'):
                self.check()
        backend['Env'] = ['SSL_CERT_FILE='+pf.CA_FILE]
        proof = self.root / 'evidence/HTTP_BACKEND_TLS_BINDING.json'
        value = pf.read(proof)
        for change in [dict(check_hostname=False), dict(verify_mode='CERT_NONE'), dict(ca_certificates=0)]:
            save(proof, dict(value, **change))
            with self.assertRaisesRegex(ValueError, 'probe_backend_effective_ca_context'):
                self.check()
        save(proof, value)
        self.ca.write_text('changed mounted CA')
        with self.assertRaisesRegex(ValueError, 'probe_backend_effective_ca_context'):
            self.check()

    def test_uds_initialization_and_actual_uid_bind_proof_are_required(self):
        path = self.root / 'evidence/UDS_VOLUME_BINDING.json'
        original = pf.read(path)
        for mutate in [lambda v:v['ownership']['after'].update(uid=0),
                       lambda v:v['ownership']['after'].update(mode='0o755'),
                       lambda v:v['unix_bind'].update(unix_listener_bind='NOT_PROVEN'),
                       lambda v:v.update(volume='other-project-uds')]:
            value = json.loads(json.dumps(original))
            mutate(value); save(path,value)
            with self.assertRaisesRegex(ValueError, 'probe_uds_owner_or_bind_proof'):
                self.check()
        save(path, original)
        path.unlink()
        with self.assertRaises(FileNotFoundError):
            self.check()


if __name__=='__main__':unittest.main()
