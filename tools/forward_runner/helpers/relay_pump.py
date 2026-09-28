"""Bounded transparent pump with TCP half-close propagation after queued bytes drain."""
import errno
import selectors
import socket
import time

BUFFER_BYTES = 65536


def pump(relay, left, right, initial=b''):
    buffers = {left: bytearray(), right: bytearray(initial)}
    peer = {left: right, right: left}
    reading = {left: True, right: True}
    write_closed = {left: False, right: False}
    selector = selectors.DefaultSelector()
    started = time.monotonic()
    eof_directions = []
    reason = 'STOP_OR_LEASE'
    for stream in peer:
        stream.setblocking(False)

    def facts():
        return dict(
            last_connection_age_ms=int((time.monotonic() - started) * 1000),
            eof_directions=list(eof_directions),
            pending_to_client_bytes=len(buffers[left]),
            pending_to_upstream_bytes=len(buffers[right]),
        )

    try:
        while relay.valid():
            relay.status()
            remaining = relay.allowance()
            for stream in peer:
                # An EOF ends this read direction only. Propagate its FIN after
                # every byte already queued for the opposite socket is delivered.
                if not reading[peer[stream]] and not buffers[stream] and not write_closed[stream]:
                    try:
                        stream.shutdown(socket.SHUT_WR)
                    except OSError as error:
                        # Both EOFs and zero pending bytes are already observed.
                        # macOS reports ENOTCONN for a redundant FIN on this socket.
                        if error.errno != errno.ENOTCONN or reading[stream]:
                            raise
                        relay.status(True, already_closed_write_direction=(
                            'client' if stream is left else 'upstream'), **facts())
                    write_closed[stream] = True
                mask = selectors.EVENT_WRITE if buffers[stream] else 0
                if reading[stream] and remaining > 0 and len(buffers[peer[stream]]) < BUFFER_BYTES:
                    mask |= selectors.EVENT_READ
                try:
                    selector.unregister(stream)
                except KeyError:
                    pass
                if mask:
                    selector.register(stream, mask)
            if not any(reading.values()) and not any(buffers.values()):
                return reason
            for key, mask in selector.select(.05):
                stream = key.fileobj
                if mask & selectors.EVENT_WRITE:
                    count = stream.send(buffers[stream])
                    del buffers[stream][:count]
                    if relay.role == 'backend' and stream is right:
                        relay.state['upstream_sent_bytes'] += count
                if mask & selectors.EVENT_READ:
                    size = min(16384, relay.allowance(), BUFFER_BYTES - len(buffers[peer[stream]]))
                    if size <= 0:
                        continue
                    data = stream.recv(size)
                    if not data:
                        reading[stream] = False
                        direction = 'client' if stream is left else 'upstream'
                        if not eof_directions:
                            reason = 'CLIENT_EOF' if stream is left else 'UPSTREAM_EOF'
                        eof_directions.append(direction)
                        relay.status(True, **{direction + "_eof_pending_to_client_bytes": len(buffers[left]),
                                             direction + "_eof_pending_to_upstream_bytes": len(buffers[right])},
                                     last_eof_direction=direction,
                                     pending_to_client_bytes_at_eof=len(buffers[left]),
                                     pending_to_upstream_bytes_at_eof=len(buffers[right]), **facts())
                        continue
                    relay.state['received_bytes'] += len(data)
                    if relay.role == 'backend' and stream is right:
                        relay.state['upstream_received_bytes'] += len(data)
                    buffers[peer[stream]].extend(data)
                    relay.last_progress = time.monotonic()
    except OSError as error:
        # Numeric errno is useful and cannot reveal a remote endpoint/header.
        relay.status(True, last_io_errno=error.errno, **facts())
        raise
    finally:
        relay.status(True, **facts())
        selector.close()
        for stream in peer:
            try:
                stream.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            stream.close()
    return reason
