"""Bind only the read-only HTTP body and its normalized ingress resource bounds."""
from pathlib import Path
import re
import sys

HERE = Path(__file__).resolve().parent
BROKER = HERE / 'http_broker' if (HERE / 'http_broker').is_dir() else HERE.parent / 'http_recovery_broker'
sys.path.insert(0, str(BROKER))
from size_contract import RECOVERY_BLOCK_BYTES

INPUT_BYTES = RECOVERY_BLOCK_BYTES
QUEUE_COUNT = 4
QUEUE_BYTES = QUEUE_COUNT * (INPUT_BYTES + 512)
CLIENT_TIMEOUT_MS = 15_000


def update_section(text, header, fields):
    if text.count(header) != 1:
        raise ValueError('probe_section_required_once:' + header)
    before, section = text.split(header, 1)
    body, separator, after = section.partition('\n[')
    for name, pattern, replacement in fields:
        body, count = re.subn(pattern, replacement, body)
        if count != 1:
            raise ValueError('probe_bound_required_once:' + name)
    return before + header + body + separator + after


def bind_config(text):
    association = '[ingestion.yellowstone_association]'
    text = update_section(text, association, [
        ('input_bytes', r'(?m)^input_bytes\s*=\s*\d+[ \t]*$', f'input_bytes = {INPUT_BYTES}'),
        ('queue', r'(?m)^queue\s*=\s*\{[ \t]*count[ \t]*=[ \t]*4[ \t]*,[ \t]*bytes[ \t]*=[ \t]*\d+[ \t]*\}[ \t]*$',
         f'queue = {{ count = {QUEUE_COUNT}, bytes = {QUEUE_BYTES} }}'),
    ])
    recovery = '[ingestion.yellowstone_http_recovery]'
    if recovery not in text:
        return text + ('\n' + recovery + '\nbroker_url="http://127.0.0.1:18765/rpc"\n'
                       'broker_token=""\nrange_slots=1024\n'
                       f'max_response_bytes={RECOVERY_BLOCK_BYTES}\ntimeout_ms={CLIENT_TIMEOUT_MS}\n'
                       'fetch_concurrency=4\n')
    return update_section(text, recovery, [
        ('timeout_ms', r'(?m)^timeout_ms[ \t]*=[ \t]*\d+[ \t]*$', f'timeout_ms={CLIENT_TIMEOUT_MS}'),
        ('max_response_bytes', r'(?m)^max_response_bytes[ \t]*=[ \t]*\d+[ \t]*$',
         f'max_response_bytes={RECOVERY_BLOCK_BYTES}'),
    ])
