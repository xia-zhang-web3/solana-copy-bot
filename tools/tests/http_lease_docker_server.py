"""Task-owned local HTTPS/broker inside Docker; no provider or signer material."""
import argparse
from collections import Counter
import json
from pathlib import Path
import signal
import threading
import time

import http_recovery_delivery_server as delivery
from broker import FrontServer
from policy import Gate


def serve(args):
    # The host generated this disposable local CA with the accepted fixture
    # helper. The accepted cached Python image intentionally has no OpenSSL CLI.
    required = ['trusted.pem', 'server.pem', 'server.key']
    if not all((args.directory / name).is_file() for name in required):
        raise ValueError('local_fixture_certificates_missing')
    delivery.certificates = lambda _: None
    fixture_save = delivery.save

    def preserve_host_clock(path, value):
        if Path(path).name in {'PROBE_CLOCK.json', 'LEASE.json'} and Path(path).exists():
            return  # Already initialized on the Mac by the actual controller.
        fixture_save(path, value)
    delivery.save = preserve_host_clock
    fixture = delivery.Fixture(args.directory, args.corpus, 'delay5', 5.15, 480,
                               args.fault_slot)
    fixture.front.server_close()
    fixture.front = FrontServer(('0.0.0.0', args.front_port), fixture.backend.server_address,
                                args.directory)
    fixture.servers[-1] = fixture.front
    fixture.start()
    stop = threading.Event()
    observations = Counter()
    refusals = []
    appended = []

    def modeled_corpus_updates():
        # Only test-owned new empty live-tail blocks are appended after restart.
        # Original saved05 bodies remain the initial immutable corpus snapshot.
        while not stop.wait(.05):
            for path in args.corpus.glob('*.json'):
                if path.stem.isdecimal() and int(path.stem) not in fixture.corpus:
                    body = json.loads(path.read_bytes())
                    if body['result']['transactions']:
                        raise ValueError('modeled_tail_requires_empty_transactions')
                    fixture.corpus[int(path.stem)] = body
                    appended.append(int(path.stem))
            if appended:
                delivery.save(args.directory / 'MODELED_CORPUS_APPENDED.json', dict(slots=appended))

    def gate_observer():
        while not stop.wait(.001):
            mode = 'valid'
            try:
                mode = (args.directory / 'TEST_MODE').read_text().strip()
            except FileNotFoundError:
                pass
            try:
                # Raw read is observation only; Gate separately invokes its real
                # authority reader and never consumes this value or exception.
                (args.directory / 'LEASE.json').read_text()
                observations[mode + ':raw_lease_ok'] += 1
            except OSError as error:
                observations[mode + ':raw_lease_' + type(error).__name__] += 1
            try:
                Gate(args.directory, fixture.policy).check()
                observations[mode + ':gate_ok'] += 1
            except Exception as error:
                observations[mode + ':gate_denied'] += 1
                if len(refusals) < 64:
                    refusals.append(dict(mode=mode, cause_type=type(error).__name__,
                                         reason=str(error), at_unix=time.time(),
                                         control_fact=getattr(error, 'control_fact', {})))
    thread = threading.Thread(target=gate_observer, daemon=True)
    thread.start()
    updates = threading.Thread(target=modeled_corpus_updates, daemon=True)
    updates.start()
    for number in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(number, lambda *_: stop.set())
    try:
        limit = time.monotonic() + 90
        while not stop.wait(.05) and not (args.directory / 'STOP_TEST').exists():
            if time.monotonic() >= limit:
                raise TimeoutError('local_docker_fixture_deadline')
    finally:
        stop.set()
        thread.join(timeout=1)
        updates.join(timeout=1)
        delivery.save(args.directory / 'GATE_OBSERVATIONS.json',
                      dict(counts=dict(observations), bounded_refusals=refusals))
        fixture.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--corpus', type=Path, required=True)
    parser.add_argument('--front-port', type=int, required=True)
    parser.add_argument('--fault-slot', type=int, required=True)
    serve(parser.parse_args())
