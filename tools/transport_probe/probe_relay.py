"""Read-only profile for the canonical relay: independent stream STOP and clock."""
import json
from pathlib import Path
import sys
import time

helpers = Path(__file__).resolve().parent / 'helpers'
if not helpers.exists():
    helpers = Path(__file__).resolve().parent.parent / 'forward_runner/helpers'
sys.path.insert(0, str(helpers))
from session_relay import Relay, read, save


class ProbeRelay(Relay):
    def __init__(self, role, directory, sockpath):
        super().__init__(role, directory, sockpath, stop_filename='STREAM_STOP')
        if self.cfg.get('role') != 'read_only_transport_probe' or self.cfg.get('max_connections') != 3:
            raise ValueError('probe_profile_mismatch')
        self.booted = time.time()

    def before_upstream_attempt(self):
        path = self.d / 'PROBE_CLOCK.json'
        if not path.exists():
            value = dict(run_id=self.cfg['run_id'], first_attempt_unix=time.time(), duration_seconds=480)
            value['deadline_unix'] = value['first_attempt_unix'] + 480
            path.write_text(json.dumps(value))
            path.chmod(0o600)

    def valid(self):
        if not super().valid():
            return False
        clock = read(self.d / 'PROBE_CLOCK.json')
        if clock is None:
            return time.time() < self.booted + 60
        return clock.get('run_id') == self.cfg['run_id'] and time.time() < clock['deadline_unix']


if __name__ == '__main__':
    ProbeRelay(sys.argv[1], Path('/control'), Path('/relay/transport.sock')).run()
