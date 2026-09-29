"""Bounded private-UDS framing with optional recovery whole-delivery deadline."""
import json
from size_contract import check_size, DEFAULT_FRAME_BYTES

MAX_FRAME = DEFAULT_FRAME_BYTES


def read_frame(sock, maximum=MAX_FRAME, delivery=None):
    head = bytearray()
    while len(head) < 16:
        if delivery is not None:
            sock.settimeout(delivery.remaining())
        part = sock.recv(16 - len(head))
        if not part:
            raise EOFError()
        head.extend(part)
    size = int(head.decode('ascii'), 16)
    check_size(size, maximum, 'frame', 'declared', 'frame_too_large')
    data = bytearray()
    while len(data) < size:
        if delivery is not None:
            sock.settimeout(delivery.remaining())
        part = sock.recv(min(65536, size-len(data)))
        if not part:
            raise EOFError()
        data.extend(part)
    return json.loads(data)


def write_frame(sock, value, maximum=MAX_FRAME, delivery=None):
    if delivery is None:
        data = json.dumps(value, separators=(',', ':')).encode()
    else:
        with delivery.phase('frame_encode'):
            data = json.dumps(value, separators=(',', ':')).encode()
    check_size(len(data), maximum, 'frame', reason='frame_too_large')
    if delivery is None:
        sock.sendall(f'{len(data):016x}'.encode() + data)
    else:
        sock.settimeout(delivery.remaining())
        with delivery.phase('unix_send'):
            sock.sendall(f'{len(data):016x}'.encode())
            sock.sendall(data)
        delivery.remaining()
