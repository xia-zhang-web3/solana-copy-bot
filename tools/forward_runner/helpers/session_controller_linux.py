"""Persistent Linux ledger owner; broker responses cross stdio, never provider sockets."""
import json
import os
from pathlib import Path
import queue
import sys
import threading

sys.path.insert(0, os.environ.get('CAPTURE_REPO', '/repo') + '/tools')
from session_capture import CaptureSession
from session_workers import job_worker
from session_budget import Exhausted


class Controller:
    def __init__(self, capture, ledger, output):
        self.session = CaptureSession(capture, ledger)
        self.output = output
        self.write_lock = threading.Lock()
        self.stop = threading.Event()
        self.pending = {}
        self.next_id = 0
        self.jobs = []

    def send(self, value):
        with self.write_lock:
            self.output.write(json.dumps(value, allow_nan=False) + '\n')
            self.output.flush()

    def once(self, service, method, params, key):
        with self.write_lock:
            self.next_id += 1
            ident = self.next_id
            response = self.pending[ident] = queue.Queue(maxsize=1)
        self.send(dict(type='broker', id=ident, args=[service, method, params, key]))
        try:
            while not self.stop.is_set():
                try:
                    value = response.get(timeout=.1)
                    if value.get('error') == 'Exhausted':
                        raise Exhausted('host_budget_exhausted')
                    if value.get('error'):
                        raise RuntimeError('host_broker_' + value['error'])
                    return value['result']
                except queue.Empty:
                    continue
            raise InterruptedError('controller_stopped')
        finally:
            self.pending.pop(ident, None)

    def start_jobs(self):
        if self.jobs:
            return {'started': False}
        for quotes in (True, False, False):
            thread = threading.Thread(target=self.run_jobs, args=(quotes,), daemon=True)
            self.jobs.append(thread)
            thread.start()
        return {'started': True, 'platform': sys.platform, 'pid': os.getpid()}

    def run_jobs(self, quotes):
        try:
            job_worker(self.session.ledger, self, self.stop, quotes)
        except Exception as exc:
            self.stop.set()
            self.send(dict(type='fatal', error=type(exc).__name__))

    def dispatch(self, op, args):
        if self.stop.is_set() and op not in ('export','summary','stop_pending','binding'):
            raise ValueError('controller_worker_stopped')
        if op == 'capacity':
            from session_capacity import sample_linux
            return sample_linux(Path('/state'), self.session.control)
        if op in ('publish', 'poll', 'observe_once'):
            return getattr(self.session, op)(*args)
        if op in ('summary', 'export', 'stop_pending'):
            return getattr(self.session.ledger, op)(*args)
        if op == 'start_jobs':
            return self.start_jobs()
        if op == 'binding':
            paths = [self.session.ledger.path]
            return dict(platform=sys.platform, pid=os.getpid(), paths=paths,
                        identities=[dict(device=Path(p).stat().st_dev,
                                         inode=Path(p).stat().st_ino) for p in paths],
                        workers_alive=[t.is_alive() for t in self.jobs])
        raise ValueError('controller_operation_not_allowed')

    def finish(self):
        self.stop.set()
        for thread in self.jobs:
            thread.join(timeout=10)
        if any(thread.is_alive() for thread in self.jobs):
            raise RuntimeError('controller_workers_did_not_stop')
        self.session.close()

    def serve(self, stream):
        try:
            for line in stream:
                value = json.loads(line)
                if value.get('type') == 'broker_result':
                    response = self.pending.get(value['id'])
                    if response:
                        response.put_nowait(value)
                    continue
                ident = value['id']
                try:
                    if value['op'] == 'close':
                        self.finish()
                        self.send(dict(type='result', id=ident, result=None))
                        return
                    result = self.dispatch(value['op'], value.get('args', []))
                    self.send(dict(type='result', id=ident, result=result))
                except Exception as exc:
                    self.send(dict(type='result', id=ident, error=type(exc).__name__,
                                   message=str(exc)))
        finally:
            if not self.stop.is_set():
                self.finish()


if __name__ == '__main__':
    if sys.platform != 'linux':
        raise SystemExit('controller_requires_linux')
    Path('/state/controller/state').mkdir(parents=True, exist_ok=True)
    Controller('/state/capture.db', '/state/virtual.db', sys.stdout).serve(sys.stdin)
