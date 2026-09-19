"""Small native fixture for the read-only Linux monitor API and exact boundaries."""
import copy
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from session_capacity import sample_linux, validate_sample

CONFIG = json.loads((Path(__file__).resolve().parents[1] / 'config/capacity.disabled.json').read_text())


class CapacityGuardTests(unittest.TestCase):
    def sample(self):
        return {'capture': {'used_bytes': CONFIG['capture']['max_bytes'],
                'max_bytes': CONFIG['capture']['max_bytes'], 'max_rows': CONFIG['capture']['max_rows'],
                'event_rows': CONFIG['capture']['max_rows'], 'status': 'running',
                'protected_wallet_union': 128},
                'state_physical_bytes': 0, 'wal_bytes': 0, 'logs_bytes': 0,
                'memory_bytes': 1, 'disk_free_bytes': CONFIG['resource_proposal']['disk_reserve_bytes']}

    def test_exact_and_overflow_boundaries(self):
        sample = self.sample()
        self.assertEqual(validate_sample(sample, CONFIG), [])
        for section, key, expected in [('capture', 'used_bytes', 'capture_bytes'),
                                       ('capture', 'event_rows', 'capture_rows'),
                                       ('capture', 'protected_wallet_union', 'protected_wallet_union')]:
            changed = copy.deepcopy(sample)
            changed[section][key] += 1
            self.assertIn('capacity_' + expected, validate_sample(changed, CONFIG))
        for field, limit, expected in [('state_physical_bytes','state_physical_bytes','state_physical'),
                ('wal_bytes','wal_bytes_included_in_state','wal'), ('logs_bytes','logs_bytes','logs'),
                ('memory_bytes','container_memory_total_bytes','memory')]:
            changed = copy.deepcopy(sample)
            changed[field] = CONFIG['resource_proposal'][limit] + 1
            self.assertIn('capacity_' + expected, validate_sample(changed, CONFIG))
        sample['disk_free_bytes'] -= 1
        self.assertIn('capacity_disk_reserve', validate_sample(sample, CONFIG))

    def test_files_union_pins_and_sample_do_not_mutate(self):
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp)
            (state / 'exports').mkdir()
            (state / 'exports/ignored').write_bytes(b'x' * 100)
            (state / 'live-wal').write_bytes(b'wal')
            (state / 'runtime').write_bytes(b'12345')
            db = sqlite3.connect(':memory:'); db.row_factory = sqlite3.Row
            db.executescript('''CREATE TABLE capture_meta(id,used_bytes,max_bytes,max_rows,status);
              INSERT INTO capture_meta VALUES(1,7,100,10,'running');
              CREATE TABLE capture_events(id); INSERT INTO capture_events VALUES(1);
              CREATE TABLE capture_requests(id,state); INSERT INTO capture_requests VALUES(1,'ACKED'),(2,'PENDING');
              CREATE TABLE capture_members(request_id,wallet); INSERT INTO capture_members VALUES(1,'a'),(2,'b');
              CREATE TABLE capture_obligations(wallet,state); INSERT INTO capture_obligations VALUES('a','OPEN'),('c','PENDING'),('d','SETTLED');''')
            control = SimpleNamespace(db=db, lock=threading.RLock())
            before = list(db.iterdump())
            with patch('session_capacity.linux_memory_bytes', return_value=123):
                sample = sample_linux(state, control)
            self.assertEqual(sample['capture']['protected_wallet_union'], 3)
            self.assertEqual(sample['state_physical_bytes'], 8)
            self.assertEqual(sample['wal_bytes'], 3)
            self.assertEqual(sample['memory_bytes'], 123)
            self.assertEqual(list(db.iterdump()), before)
            db.close()


if __name__ == '__main__':
    unittest.main()
