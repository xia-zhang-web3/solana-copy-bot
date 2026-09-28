"""Write-ahead, fail-closed attempt reservations for the local HTTP broker."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time

HTTP_CAP_NANOUSD = 1_500_000_000
RPC_CAP_CU = 2_500_000
RPC_NANOUSD_PER_CU = 525  # Alchemy PAYG: $0.525 / 1,000,000 CU.
CU = {
    'getAccountInfo': 10, 'getBalance': 10, 'getGenesisHash': 10,
    'getMinimumBalanceForRentExemption': 10, 'getTokenAccountsByOwner': 10,
    'getFeeForMessage': 20, 'getLatestBlockhash': 20,
    'getMultipleAccounts': 20, 'getSignatureStatuses': 20,
    'getSlot': 20, 'getTokenSupply': 20, 'sendTransaction': 20,
    'getProgramAccounts': 20, 'isBlockhashValid': 20,
    'simulateTransaction': 20, 'getTransaction': 40,
    'getBlocks': 10, 'getBlock': 40, 'getSignaturesForAddress': 40,
    'getTokenAccountsByOwnerAtSlot': 40,
}


class Refused(Exception):
    pass


def policy_hash(policy):
    # The hash binds endpoint credentials without persisting their plaintext.
    return hashlib.sha256(json.dumps(policy, sort_keys=True).encode()).hexdigest()


class Ledger:
    def __init__(self, path, policy):
        self.path = Path(path)
        self.policy = policy
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        marker = self.path.with_suffix('.binding.json')
        if marker.exists() and not self.path.exists():
            raise Refused('ledger_lost_after_first_open')
        old = os.umask(0o077)
        try:
            self.db = sqlite3.connect(self.path, timeout=5, isolation_level=None,
                                      check_same_thread=False)
        finally:
            os.umask(old)
        os.chmod(self.path, 0o600)
        self.db.execute('PRAGMA busy_timeout=5000')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS head '
                        '(id INTEGER PRIMARY KEY CHECK(id=1), run_id TEXT NOT NULL, '
                        'policy_hash TEXT NOT NULL, usd_nano INTEGER NOT NULL, '
                        'rpc_cu INTEGER NOT NULL, attempts INTEGER NOT NULL)')
        self.db.execute('CREATE TABLE IF NOT EXISTS attempts '
                        '(n INTEGER PRIMARY KEY, route TEXT NOT NULL, method TEXT NOT NULL, '
                        'usd_nano INTEGER NOT NULL, rpc_cu INTEGER NOT NULL, at_unix REAL NOT NULL)')
        self.db.execute('BEGIN IMMEDIATE')
        try:
            row = self.db.execute('SELECT run_id,policy_hash FROM head WHERE id=1').fetchone()
            expected = (policy['run_id'], policy_hash(policy))
            if row is None:
                self.db.execute('INSERT INTO head VALUES(1,?,?,0,0,0)', expected)
            elif row != expected:
                raise Refused('ledger_binding_changed')
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK')
            raise
        binding = {'run_id': policy['run_id'], 'policy_hash': policy_hash(policy)}
        try:
            fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            pass
        else:
            with os.fdopen(fd, 'w') as stream:
                json.dump(binding, stream)
                stream.flush()
                os.fsync(stream.fileno())
            directory = os.open(marker.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        if json.loads(marker.read_text()) != binding:
            raise Refused('ledger_marker_binding_changed')

    def reserve(self, route, method, quote_price_nano=None):
        if route != 'rpc' or method not in ('getBlock', 'getBlocks'):
            raise Refused('read_only_recovery_method_required')
        if route == 'rpc':
            cu = CU.get(method)
            if cu is None:
                raise Refused('unknown_rpc_method_or_price')
            cost = cu * RPC_NANOUSD_PER_CU
        elif route == 'quote':
            if type(quote_price_nano) is not int or quote_price_nano <= 0:
                raise Refused('unknown_quote_price')
            cu, cost = 0, quote_price_nano
        else:
            raise Refused('unknown_route')
        self.db.execute('BEGIN IMMEDIATE')
        try:
            used, rpc_used, n = self.db.execute(
                'SELECT usd_nano,rpc_cu,attempts FROM head WHERE id=1').fetchone()
            if (n >= self.policy['max_rpc_attempts']
                    or rpc_used + cu > self.policy['max_rpc_cu']
                    or used + cost + self.policy['prior_http_nano_usd'] > HTTP_CAP_NANOUSD
                    or used + cost + self.policy['prior_model_nano_usd'] + self.policy['stream_reserved_nano_usd'] > 50_000_000_000
                    or rpc_used + cu > RPC_CAP_CU):
                raise Refused('http_or_rpc_cap_exhausted')
            n += 1
            self.db.execute('INSERT INTO attempts VALUES(?,?,?,?,?,?)',
                            (n, route, method, cost, cu, time.time()))
            self.db.execute('UPDATE head SET usd_nano=?,rpc_cu=?,attempts=? WHERE id=1',
                            (used + cost, rpc_used + cu, n))
            self.db.execute('COMMIT')
            return n
        except BaseException:
            self.db.execute('ROLLBACK')
            raise

    def totals(self):
        return self.db.execute('SELECT usd_nano,rpc_cu,attempts FROM head WHERE id=1').fetchone()

    def close(self):
        self.db.close()
