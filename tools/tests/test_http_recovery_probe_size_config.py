"""Causal mismatches between HTTP body, normalized queue and installed role caps."""
import unittest

import test_http_recovery_probe_preflight as preflight_fixture
from config_bounds import bind_config, INPUT_BYTES, QUEUE_BYTES, CLIENT_TIMEOUT_MS


class SizeConfig(unittest.TestCase):
    def test_config_binding_preserves_financial_and_unrelated_fields(self):
        source = ('[execution]\nenabled=false\nmax_submit_attempts=1\n'
                  '[ingestion.yellowstone_association]\n'
                  'input_bytes=8388608\nqueue={count=4,bytes=8388608}\n'
                  'blocks={count=384,bytes=805306368}\nmetadata_bytes=100663296\n'
                  '[ingestion.yellowstone_http_recovery]\n'
                  'timeout_ms=15000\nmax_response_bytes=8388608\nfetch_concurrency=4\n'
                  '[risk]\nmax_position_sol=0.10\n')
        result = bind_config(source)
        self.assertIn(f'input_bytes = {INPUT_BYTES}', result)
        self.assertIn(f'queue = {{ count = 4, bytes = {QUEUE_BYTES} }}', result)
        self.assertIn('max_response_bytes=16777216', result)
        self.assertIn(f'timeout_ms={CLIENT_TIMEOUT_MS}', result)
        for unchanged in ['enabled=false', 'max_submit_attempts=1',
                          'blocks={count=384,bytes=805306368}', 'metadata_bytes=100663296',
                          'fetch_concurrency=4', 'max_position_sol=0.10']:
            self.assertIn(unchanged, result)

    def test_parser_queue_and_actual_role_caps_cannot_drift(self):
        fixture = preflight_fixture.PreflightTests()
        fixture.setUp()
        try:
            fixture.check()
            path = fixture.root / 'config/read-only.toml'
            for text in [fixture.config.replace('input_bytes=16777216', 'input_bytes=8388608'),
                         fixture.config.replace('bytes=67110912', 'bytes=8388608'),
                         fixture.config.replace('max_response_bytes=16777216', 'max_response_bytes=8388608')]:
                path.write_text(text)
                with self.assertRaises(ValueError):
                    fixture.check()
            path.write_text(fixture.config)
            for role in ['http-front', 'http-backend', 'observation-app']:
                host = fixture.metadata[fixture.ids[role]]['HostConfig']
                for key in ['Memory', 'MemorySwap', 'NanoCpus']:
                    original = host[key]
                    host[key] = 256 * 1024**2 if key != 'NanoCpus' else 250_000_000
                    with self.assertRaisesRegex(ValueError, 'probe_measured_resource_limits'):
                        fixture.check()
                    host[key] = original
            for role in ['http-front', 'http-backend']:
                config = fixture.metadata[fixture.ids[role]]['Config']
                original = config['Env']
                config['Env'] = [item for item in original if not item.startswith('MALLOC_ARENA_MAX=')]
                with self.assertRaisesRegex(ValueError, 'probe_measured_http_allocator'):
                    fixture.check()
                config['Env'] = original
        finally:
            fixture.tearDown()


if __name__ == '__main__':
    unittest.main()
