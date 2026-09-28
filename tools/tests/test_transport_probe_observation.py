"""Controls for the daemon's installed compact output and JSON compatibility."""
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'transport_probe'))
from probe_observation import fields, summarize


class ObservationTests(unittest.TestCase):
    def test_installed_compact_and_json_keep_error_and_durable_slots(self):
        event = dict(message='durable ingress transport boundary', error_message='local EOF',
                     grpc_code='Internal', last_durably_stored_parent_slot=123)
        payload = json.dumps(event)
        for line in [payload, json.dumps({'fields': event}),
                     '\x1b[33m2026-09-28T19:36:04.811405Z  WARN\x1b[0m ' + payload]:
            self.assertEqual(fields(line), event)
            self.assertEqual(summarize(line)['transport'][0]['error_message'], 'local EOF')
            self.assertEqual(summarize(line)['transport'][0]['last_durably_stored_parent_slot'], 123)

    def test_garbage_and_wrong_shapes_are_explicitly_unparsed(self):
        lines = ['garbage {"message":"durable ingress transport boundary"}',
                 '{"fields":[]}', '[]', '2026-09-28T19:36:04Z WARN {broken}']
        value = summarize('\n'.join(lines))
        self.assertEqual(value, dict(transport=[], ingress=None, unparsed_lines=4))

    def test_transport_tail_is_bounded_and_funnel_is_preserved(self):
        lines = [json.dumps(dict(message='durable ingress transport boundary', connection_age_ms=n))
                 for n in range(20)]
        ingress = dict(message='durable ingress funnel', received_blocks=7)
        value = summarize('\n'.join(lines + [json.dumps(ingress)]))
        self.assertEqual(len(value['transport']), 12)
        self.assertEqual(value['transport'][0]['connection_age_ms'], 8)
        self.assertEqual(value['ingress'], ingress)
