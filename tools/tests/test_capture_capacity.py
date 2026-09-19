"""Persist explicit capacity only; old limits and failure accounting stay bounded."""
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from capture_virtual.control import CaptureControl, validate_bounds


class CapacityTests(unittest.TestCase):
    def test_exact_ceiling_and_invalid_values(self):
        validate_bounds(4_000_000, 68_719_476_736)
        for rows, size in [(4_000_001, 1), (1, 68_719_476_737), (0, 1),
                           (1, 0), (-1, 1), (True, 1), (1, 1.0)]:
            with self.assertRaises(ValueError):
                validate_bounds(rows, size)

    def test_explicit_limits_persist_without_upgrading_old_database(self):
        with tempfile.TemporaryDirectory() as temp:
            for name, rows, size in [('old', 1_000_000, 1_073_741_824),
                                     ('planned', 2_000_000, 51_539_607_552)]:
                path = Path(temp) / name
                control = CaptureControl.create(path, max_rows=rows, max_bytes=size)
                control.close()
                control = CaptureControl(path)
                meta = control.db.execute('SELECT max_rows,max_bytes FROM capture_meta').fetchone()
                self.assertEqual(tuple(meta), (rows, size))
                control.close()


if __name__ == '__main__':
    unittest.main()
