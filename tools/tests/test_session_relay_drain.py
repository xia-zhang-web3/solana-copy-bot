"""Causal local controls for the transparent relay; no external endpoints."""
import importlib.util
from pathlib import Path
import socket
import threading
import time
import unittest

HELPERS = Path(__file__).resolve().parents[1] / 'forward_runner' / 'helpers'
spec = importlib.util.spec_from_file_location('relay_pump', HELPERS / 'relay_pump.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class LocalRelay:
    role = 'backend'

    def __init__(self, seconds=3):
        self.state = {'received_bytes': 0, 'upstream_received_bytes': 0, 'upstream_sent_bytes': 0}
        self.last_progress = time.monotonic()
        self.until = self.last_progress + seconds
        self.stopped = False

    def valid(self):
        return not self.stopped and time.monotonic() < self.until

    def allowance(self):
        return 2**30

    def status(self, force=False, **fields):
        self.state.update(fields)


def receive(stream):
    out = bytearray()
    while True:
        data = stream.recv(8192)
        if not data:
            return bytes(out)
        out.extend(data)


class RelayDrainTests(unittest.TestCase):
    def test_client_half_close_drains_request_and_receives_response(self):
        client, left = socket.socketpair()
        right, upstream = socket.socketpair()
        relay = LocalRelay()
        request = b'r' * 32768
        response = b's' * 32768
        client.shutdown(socket.SHUT_WR)
        result = {}
        worker = threading.Thread(target=lambda: result.update(reason=module.pump(relay, left, right, request)))
        worker.start()
        self.assertEqual(receive(upstream), request)
        upstream.sendall(response)
        upstream.shutdown(socket.SHUT_WR)
        self.assertEqual(receive(client), response)
        worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(result['reason'], 'CLIENT_EOF')
        self.assertEqual(relay.state['client_eof_pending_to_upstream_bytes'], len(request))
        self.assertEqual(relay.state['upstream_sent_bytes'], len(request))
        self.assertEqual(relay.state['upstream_received_bytes'], len(response))
        self.assertEqual(relay.state['received_bytes'], len(response))  # initial was already charged
        self.assertEqual(relay.state['eof_directions'], ['client', 'upstream'])
        self.assertEqual(relay.state['pending_to_client_bytes'], 0)
        client.close()
        upstream.close()

    def test_upstream_half_close_drains_large_response_before_fin(self):
        client, left = socket.socketpair()
        right, upstream = socket.socketpair()
        left.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        relay = LocalRelay()
        payload = b'z' * (4 * 65536)
        result = {}
        worker = threading.Thread(target=lambda: result.update(reason=module.pump(relay, left, right)))
        writer = threading.Thread(target=lambda: (upstream.sendall(payload), upstream.shutdown(socket.SHUT_WR)))
        worker.start()
        writer.start()
        time.sleep(.03)  # controlled slow downstream, enough to fill relay's bounded buffer
        self.assertEqual(receive(client), payload)
        client.shutdown(socket.SHUT_WR)
        self.assertEqual(receive(upstream), b'')
        worker.join(3)
        writer.join(3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(result['reason'], 'UPSTREAM_EOF')
        self.assertEqual(relay.state['received_bytes'], len(payload))
        self.assertEqual(relay.state['upstream_received_bytes'], len(payload))
        self.assertEqual(relay.state['pending_to_client_bytes'], 0)
        client.close()
        upstream.close()

    def test_receive_credit_never_increases_during_drain(self):
        client, left = socket.socketpair()
        right, upstream = socket.socketpair()
        relay = LocalRelay()
        credit = 32768
        relay.allowance = lambda: max(0, credit - relay.state['received_bytes'])
        result = {}
        worker = threading.Thread(target=lambda: result.update(reason=module.pump(relay, left, right)))
        worker.start()
        def write_beyond_credit():
            try:
                upstream.sendall(b'x' * (2 * credit))
            except (BrokenPipeError, ConnectionResetError):
                pass  # deliberate STOP may leave the uncredited suffix unread
        writer = threading.Thread(target=write_beyond_credit)
        writer.start()
        received = bytearray()
        client.settimeout(1)
        while len(received) < credit:
            received.extend(client.recv(credit - len(received)))
        relay.stopped = True
        worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(received, b'x' * credit)
        self.assertEqual(relay.state['received_bytes'], credit)
        self.assertEqual(relay.state['upstream_received_bytes'], credit)
        self.assertEqual(result['reason'], 'STOP_OR_LEASE')
        writer.join(3)
        self.assertFalse(writer.is_alive())
        client.close()
        upstream.close()

    def test_stop_does_not_keep_draining_without_authority(self):
        client, left = socket.socketpair()
        right, upstream = socket.socketpair()
        relay = LocalRelay()
        relay.stopped = True
        self.assertEqual(module.pump(relay, left, right, b'x' * 32768), 'STOP_OR_LEASE')
        self.assertEqual(receive(upstream), b'')
        self.assertEqual(relay.state['pending_to_upstream_bytes'], 32768)
        self.assertEqual(relay.state['received_bytes'], 0)
        client.close()
        upstream.close()


if __name__ == '__main__':
    unittest.main()
