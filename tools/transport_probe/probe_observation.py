"""Read the installed daemon's JSON and compact tracing event formats."""
import json
import re

ANSI = re.compile(r'\x1b\[[0-9;]*m')
COMPACT = re.compile(r'^\d{4}-\d{2}-\d{2}T\S+\s+(?:TRACE|DEBUG|INFO|WARN|ERROR)\s+(\{.*\})$')
TRANSPORT_KEYS = ['stage', 'class', 'grpc_code', 'message', 'error_message', 'causes',
    'connection_age_ms', 'last_received_transaction_slot', 'last_received_block_slot',
    'last_emitted_parent_slot', 'last_durably_stored_parent_slot']


def fields(line):
    line = ANSI.sub('', line).strip()
    if not line.startswith('{'):
        match = COMPACT.fullmatch(line)
        if not match:
            return None
        line = match.group(1)
    try:
        value = json.loads(line)
    except ValueError:
        return None
    if not isinstance(value, dict):
        return None
    value = value.get('fields', value)
    return value if isinstance(value, dict) else None


def summarize(text):
    transport, ingress, unparsed = [], None, 0
    for line in text.splitlines():
        event = fields(line)
        if event is None:
            unparsed += 1
            continue
        if event.get('message') == 'durable ingress transport boundary':
            transport.append({key: event.get(key) for key in TRANSPORT_KEYS})
        if event.get('message') == 'durable ingress funnel':
            ingress = event
    return dict(transport=transport[-12:], ingress=ingress, unparsed_lines=unparsed)
