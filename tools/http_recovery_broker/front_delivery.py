"""Whole response delivery and closed completion facts for the HTTP front."""
import base64
import binascii
from contextlib import nullcontext

from budget import Refused
from diagnostics import broker_fact
from evidence import archive_delivery
from policy import response_limit
from size_contract import check_size


def phase(delivery, name):
    return delivery.phase(name) if delivery is not None else nullcontext()


def deliver_response(handler, result, route, rpc_method):
    try:
        with phase(handler.delivery, 'front_decode'):
            data = base64.b64decode(result['body'], validate=True)
        check_size(len(data), response_limit(route, rpc_method, handler.server.profile),
                   'front_body', reason='response_too_large')
    except ValueError as error:
        if hasattr(error, 'size_fact'):
            trace = result.get('broker_trace', {})
            fact = broker_fact('failed', rpc_method, trace.get('reservation_id'), 'front_decode', error,
                               delivery=handler.delivery)
            handler._broker_error(502, fact, persist=True)
            return
        handler._error(502, 'upstream_unavailable')
        return
    except (KeyError, TypeError, binascii.Error):
        handler._error(502, 'upstream_unavailable')
        return
    if handler.delivery is None:
        handler.send_response(result['status'])
        for key, value in result['headers'].items():
            handler.send_header(key, value)
        handler.send_header('Content-Length', str(len(data)))
        handler.send_header('Connection', 'close')
        handler.end_headers()
        handler.wfile.write(data)
        handler.close_connection = True
        return
    trace = result.get('broker_trace', {})
    reservation = trace.get('reservation_id')
    stage = 'front_headers'
    fact, outcome = None, 'response'
    try:
        handler.connection.settimeout(handler.delivery.remaining())
        with phase(handler.delivery, stage):
            handler.send_response(result['status'])
            handler._trace_headers(reservation)
            for key, value in result['headers'].items():
                handler.send_header(key, value)
            handler.send_header('Content-Length', str(len(data)))
            handler.send_header('Connection', 'close')
            handler.end_headers()
        stage = 'front_body'
        with phase(handler.delivery, stage):
            handler.wfile.write(data)
            handler.wfile.flush()
        if handler.delivery is not None:
            handler.delivery.remaining()
    except (OSError, Refused) as error:
        outcome = 'failed'
        fact = broker_fact(outcome, rpc_method, reservation, stage, error,
                           result['status'], handler.delivery)
    finally:
        archive_delivery(handler.server.control, reservation, 'front',
                         handler.delivery.trace(rpc_method, reservation), outcome, trace, fact)
        handler.close_connection = True
