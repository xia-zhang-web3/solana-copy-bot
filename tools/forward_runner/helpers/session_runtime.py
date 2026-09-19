"""Host lifecycle handle; virtual SQLite and actual job workers are Linux-owned."""
from session_common import L, T, save
from session_controller_transport import ControllerTransport
from session_sqlite import ensure_bridge
import session_docker as docker


class LinuxRuntime:
    def __init__(self, broker):
        cid = ensure_bridge()
        self.log = (L/'logs/controller.log').open('a')
        command = docker.CLI + ['exec', '-i', '-e', 'CAPTURE_REPO=/repo',
            '-e', 'FORWARD_CONTEXT=/evidence/CONTEXT.linux.json', cid,
            '/usr/bin/python3', '-B', '/helpers/session_controller_linux.py']
        self.transport = ControllerTransport(command, broker, env=docker.ENV, stderr=self.log)
        try:
            binding = self.transport.request('binding')
            if binding['platform'] != 'linux' or binding['paths'] != ['/state/virtual.db']:
                raise ValueError('controller_storage_platform_mismatch')
            save(T/'evidence/CONTROLLER_BINDING.json', binding)
            self.transport.request('start_jobs')
        except BaseException:
            self.close()
            raise

    def publish(self, decision): return self.transport.request('publish', decision)
    def poll(self): return self.transport.request('poll')
    def observe_once(self): return self.transport.request('observe_once')
    def capacity(self): return self.transport.request('capacity')
    def summary(self): return self.transport.request('summary')
    def export(self): return self.transport.request('export')
    def stop_pending(self, reason): return self.transport.request('stop_pending', reason)

    def close(self):
        try:
            self.transport.close()
        finally:
            self.log.close()
