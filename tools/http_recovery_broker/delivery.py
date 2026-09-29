"""Bounded whole-delivery clocks and closed stage facts, without provider text."""
from contextlib import contextmanager
import json
import math
from pathlib import Path
import time

from budget import Refused
from control_reader import read_clock, safe_control_fact

UPSTREAM_SECONDS = 8.0
DELIVERY_SECONDS = 12.0
STAGES = {'request', 'gate', 'reservation', 'connect', 'upstream_headers',
          'upstream_body', 'archive', 'frame_encode', 'unix_send', 'unix_receive',
          'front_decode', 'front_headers', 'front_body', 'response_sanitize'}


class DeadlineExceededTimeout(TimeoutError):
    pass


class Delivery:
    def __init__(self, control, request=None):
        self.started = time.monotonic()
        self.end = self.started + DELIVERY_SECONDS
        self.session_end = None
        self.session_deadline_unix_ms = None
        self.timings = {}
        self.control_read_recovery = None
        self.request_id = self.slot = None
        if request is not None:
            value = json.loads(request)
            identity = value.get('id')
            params = value.get('params', [])
            self.request_id = identity if type(identity) is int and 0 <= identity <= 2**64 - 1 else None
            self.slot = params[0] if params and type(params[0]) is int and params[0] > 0 else None
        try:
            clock = read_clock(control)
            if (type(clock['deadline_unix']) not in {int, float} or
                    not math.isfinite(clock['deadline_unix'])):
                raise ValueError()
            remaining = clock['deadline_unix'] - time.time()
            if (clock.get('duration_seconds') != 480 or
                    clock['deadline_unix'] != clock['first_attempt_unix'] + 480):
                raise ValueError()
            self.session_end = self.started + remaining
            self.session_deadline_unix_ms = int(clock['deadline_unix'] * 1000)
            self.end = min(self.end, self.session_end)
        except (OSError, ValueError, KeyError, TypeError):
            raise Refused('read_only_clock_or_lease_invalid') from None

    def remaining(self, end=None):
        now = time.monotonic()
        if self.session_end is not None and now >= self.session_end:
            raise Refused('session_deadline_exhausted')
        remaining = min(self.end, end if end is not None else self.end) - now
        if remaining <= 0:
            raise DeadlineExceededTimeout('deadline_exhausted')
        return remaining

    @contextmanager
    def phase(self, name):
        assert name in STAGES
        start = time.monotonic()
        try:
            yield
        finally:
            self.timings[name] = self.timings.get(name, 0) + round((time.monotonic() - start) * 1000)

    def facts(self):
        value = {'request_id': self.request_id, 'slot': self.slot,
                'elapsed_ms': max(0, round((time.monotonic() - self.started) * 1000)),
                'deadline_ms': max(0, round((self.end - self.started) * 1000)),
                'session_deadline_unix_ms': self.session_deadline_unix_ms,
                'timings_ms': dict(self.timings)}
        if self.control_read_recovery is not None:
            value['control_read_recovery'] = safe_control_fact(self.control_read_recovery)
        return value

    def trace(self, method, reservation):
        return dict(schema='http_recovery_delivery_v1', method=method,
                    reservation_id=reservation, **self.facts())
