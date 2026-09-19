"""Multiplex controller commands and metered broker callbacks over Docker stdin."""
import json
import queue
import subprocess
import threading
import time


class ControllerTransport:
    def __init__(self, command, broker, *, env=None, stderr=None):
        self.broker = broker
        self.lock = threading.Lock()
        self.pending = {}
        self.sequence = 0
        self.failure = None
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=stderr, text=True, bufsize=1, env=env)
        self.reader = threading.Thread(target=self.read, daemon=True)
        self.reader.start()

    def write(self, value):
        with self.lock:
            self.process.stdin.write(json.dumps(value, allow_nan=False) + '\n')
            self.process.stdin.flush()

    def read(self):
        try:
            for line in self.process.stdout:
                value = json.loads(line)
                if value['type'] == 'broker':
                    threading.Thread(target=self.reply, args=(value,), daemon=True).start()
                elif value['type'] == 'fatal':
                    raise ValueError('linux_worker_' + value['error'])
                else:
                    target = self.pending.get(value['id'])
                    if target:
                        target.put(value)
            self.failure = self.failure or 'controller_pipe_closed'
        except Exception as exc:
            self.failure = type(exc).__name__ + ':' + str(exc)

    def reply(self, value):
        result = dict(type='broker_result', id=value['id'])
        try:
            result['result'] = self.broker.once(*value['args'])
        except Exception as exc:
            result['error'] = type(exc).__name__
        try:
            self.write(result)
        except (OSError, ValueError):
            self.failure = 'controller_broker_response_failed'

    def request(self, op, *args, timeout=30):
        with self.lock:
            self.sequence += 1
            ident = self.sequence
            response = self.pending[ident] = queue.Queue(maxsize=1)
        try:
            self.write(dict(id=ident, op=op, args=list(args)))
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                try:
                    value = response.get(timeout=.1)
                    if value.get('error'):
                        raise ValueError('linux_controller_' + value['error'] + ':' + value.get('message', ''))
                    return value['result']
                except queue.Empty:
                    if self.failure:
                        raise ValueError(self.failure)
            raise TimeoutError('linux_controller_' + op + '_deadline')
        finally:
            self.pending.pop(ident, None)

    def close(self):
        try:
            if self.process.poll() is None and not self.failure:
                self.request('close', timeout=15)
        finally:
            self.process.stdin.close()
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.process.terminate()
                self.process.wait(timeout=5)
            self.reader.join(timeout=2)
