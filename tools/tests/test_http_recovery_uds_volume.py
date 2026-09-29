"""Closed Docker boundary controls for task-owned initialization, no live Docker."""
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

SOURCE = Path(__file__).resolve().parents[1] / 'http_recovery_probe/uds_volume.py'
spec = importlib.util.spec_from_file_location('probe_uds', SOURCE)
uds = importlib.util.module_from_spec(spec)
spec.loader.exec_module(uds)
RUN = 'copybot-run15-http-recovery-probe-904'


class DockerBoundary:
    def __init__(self):
        self.calls = []
        self.volume = None
        self.users = ''
        self.init_failure = False

    def __call__(self, args):
        self.calls.append(args)
        value = SimpleNamespace(returncode=0, stdout='', stderr='')
        if args[:2] == ['image','inspect']:
            value.stdout = json.dumps([dict(Id=uds.IMAGE, Os='linux', Architecture='amd64')])
        elif args[0] == 'ps':
            value.stdout = self.users
        elif args[:2] == ['volume','inspect']:
            if self.volume is None:
                value.returncode=1;value.stderr='Error response from daemon: No such volume: '+RUN+'-uds'
            else:
                value.stdout = json.dumps([self.volume])
        elif args[:2] == ['volume','create']:
            self.volume = dict(Name=RUN+'-uds', Driver='local', Options=None,
                               Labels={uds.LABEL:RUN, uds.ROLE:'uds'})
            value.stdout = RUN+'-uds'
        elif args[0] == 'run':
            if args[-1] == uds.INITIALIZE:
                if self.init_failure:
                    value.returncode=1;value.stderr='ValueError: nonempty_owned_volume_refused'
                else:
                    value.stdout=json.dumps(dict(before=dict(uid=0,gid=0,mode='0o755',entries=0),
                                                  after=dict(uid=501,gid=20,mode='0o700')))
            elif args[-1] == uds.BIND:
                value.stdout=json.dumps(dict(unix_listener_bind='PASS',uid=501,gid=20,network='none',provider_calls=0))
            else:
                raise AssertionError('unexpected local helper')
        else:
            raise AssertionError('unexpected Docker action')
        return value


class OwnedUDS(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.evidence = Path(self.tmp.name)
        self.engine = DockerBoundary()

    def tearDown(self):
        self.tmp.cleanup()

    def test_fresh_owned_volume_initialized_then_actual_uid_bind_proven(self):
        value=uds.prepare_uds(RUN,self.evidence,self.engine)
        self.assertEqual(value['status'],'PASS_FRESH_OWNED_UDS')
        roles=[c for c in self.engine.calls if c[0]=='run']
        self.assertEqual(len(roles),2)
        for args in roles:
            self.assertEqual(args[args.index('--network')+1],'none')
            self.assertIn('--read-only',args)
            self.assertEqual(args[args.index('--mount')+1], 'type=volume,src='+RUN+'-uds,dst=/relay')
        self.assertEqual(roles[1][roles[1].index('--user')+1],'501:20')
        self.assertNotIn('--cap-add',roles[1])
        self.assertTrue((self.evidence/'UDS_VOLUME_BINDING.json').is_file())

    def test_foreign_or_attached_volume_refuses_before_initialize(self):
        cases=[dict(Name=RUN+'-uds',Driver='local',Options=None,Labels={uds.LABEL:'another-run'}),
               dict(Name=RUN+'-uds',Driver='local',Options={'device':'shared'},Labels={uds.LABEL:RUN,uds.ROLE:'uds'})]
        for volume in cases:
            self.engine=DockerBoundary();self.engine.volume=volume
            with self.assertRaisesRegex(ValueError,'ownership_or_isolation'):
                uds.prepare_uds(RUN,self.evidence,self.engine)
            self.assertFalse(any(c[0]=='run' for c in self.engine.calls))
        self.engine=DockerBoundary();self.engine.users='existing-cid'
        with self.assertRaisesRegex(ValueError,'already_attached'):
            uds.prepare_uds(RUN,self.evidence,self.engine)
        self.assertFalse(any(c[0]=='run' for c in self.engine.calls))

    def test_local_initialization_failure_is_preserved_and_only_owned_scope_retries(self):
        self.engine.init_failure=True
        with self.assertRaises(uds.PreparationFailure):
            uds.prepare_uds(RUN,self.evidence,self.engine)
        fact=json.loads((self.evidence/'UDS_VOLUME_FAILURE.json').read_text())
        self.assertEqual((fact['action'],fact['exit_code']),('initialize',1))
        self.engine.init_failure=False
        self.assertEqual(uds.prepare_uds(RUN,self.evidence,self.engine)['status'],'PASS_FRESH_OWNED_UDS')
        self.assertEqual(sum(c[:2]==['volume','create'] for c in self.engine.calls),1)

    def test_unrelated_run_id_is_rejected_without_docker(self):
        with self.assertRaisesRegex(ValueError,'exact_task_run'):
            uds.prepare_uds('another-project',self.evidence,self.engine)
        self.assertEqual(self.engine.calls,[])


if __name__ == '__main__':
    unittest.main()
