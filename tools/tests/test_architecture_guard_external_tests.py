"""External test wiring is allowed; inline production test bodies still refuse."""
import importlib.util
from pathlib import Path
import unittest

PATH = Path(__file__).resolve().parents[1] / 'lib/architecture_guard/rust_metrics.py'
SPEC = importlib.util.spec_from_file_location('rust_metrics', PATH)
METRICS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(METRICS)


class ExternalTests(unittest.TestCase):
    def test_external_module_and_later_production_body(self):
        source = '#[cfg(test)]\n#[path="separate_tests.rs"]\nmod tests;\nfn live() {}\n'
        self.assertEqual(METRICS.inline_metrics(source), (0, 0, 0))

    def test_inline_body_remains_detected(self):
        source = '#[cfg(test)]\nmod tests {\n#[test] fn check() {}\n}\n'
        self.assertEqual(METRICS.inline_metrics(source), (1, 3, 1))
