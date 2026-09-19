"""Read-only Linux capacity sample and explicit, fail-closed session stop reasons."""
import os
from pathlib import Path
import shutil


def tree_bytes(root, exclude_exports=False):
    total = wal = 0
    root = Path(root)
    if not root.is_dir():
        raise ValueError('capacity_directory_missing')
    for directory, names, files in os.walk(root):
        if any((Path(directory) / name).is_symlink() for name in names):
            raise ValueError('capacity_symlink_not_supported')
        names[:] = [name for name in names
                    if not (exclude_exports and name == 'exports')]
        for name in files:
            path = Path(directory) / name
            if path.is_symlink():
                raise ValueError('capacity_symlink_not_supported')
            try:
                size = path.stat().st_size
            except FileNotFoundError:
                # A removed WAL or atomic metadata temporary is no longer consuming space.
                continue
            total += size
            if name.endswith('-wal'):
                wal += size
    return total, wal


def linux_memory_bytes():
    """Container cgroup usage when available; otherwise explicit unknown."""
    for filename in ('/sys/fs/cgroup/memory.current',
                     '/sys/fs/cgroup/memory/memory.usage_in_bytes'):
        path = Path(filename)
        if path.is_file():
            return int(path.read_text().strip())
    return None


def sample_linux(state, control, logs=None):
    """Call inside Linux with the actual CaptureControl, never a host DB export.

    The shared control lock serializes its SQLite connection; one SELECT takes one
    SQLite snapshot. Errors propagate so the supervisor stops rather than skips a
    required capacity sample. Does not checkpoint, evict, release pins or write SQL.
    """
    state = Path(state)
    state_bytes, wal_bytes = tree_bytes(state, exclude_exports=True)
    logs_bytes = tree_bytes(logs)[0] if logs is not None else 0
    with control.lock:
        row = control.db.execute('''SELECT used_bytes,max_bytes,max_rows,status,
          (SELECT count(*) FROM capture_events) AS event_rows,
          (SELECT count(*) FROM (
            SELECT m.wallet FROM capture_members m JOIN capture_requests r
              ON r.id=m.request_id WHERE r.state IN ('ACKED','PENDING')
            UNION SELECT wallet FROM capture_obligations WHERE state!='SETTLED'
          )) AS protected_wallet_union FROM capture_meta WHERE id=1''').fetchone()
        if row is None:
            raise ValueError('capacity_meta_missing')
        capture = dict(row)
    return {'capture': capture, 'state_physical_bytes': state_bytes,
            'wal_bytes': wal_bytes, 'logs_bytes': logs_bytes,
            'disk_free_bytes': shutil.disk_usage(state).free,
            'memory_bytes': linux_memory_bytes()}


def validate_sample(sample, config):
    """Return reasons; any reason means stop the entire session, preserving state.

    Capacity rows/bytes also have synchronous Rust enforcement. File and memory
    samples are periodic stop guards, not exact byte quotas or peak guarantees.
    """
    reasons = []
    limits = config['resource_proposal']
    capture = sample['capture']
    if capture['status'] == 'failed':
        reasons.append('capture_failed')
    for observed, bound, label in (
        (capture['used_bytes'], config['capture']['max_bytes'], 'capture_bytes'),
        (capture['event_rows'], config['capture']['max_rows'], 'capture_rows'),
        (capture['protected_wallet_union'], config['planning_envelope']['max_protected_wallet_union'], 'protected_wallet_union'),
        (sample['state_physical_bytes'], limits['state_physical_bytes'], 'state_physical'),
        (sample['wal_bytes'], limits['wal_bytes_included_in_state'], 'wal'),
        (sample['logs_bytes'], limits['logs_bytes'], 'logs'),
    ):
        if observed > bound:
            reasons.append('capacity_' + label)
    if capture['max_bytes'] != config['capture']['max_bytes'] or capture['max_rows'] != config['capture']['max_rows']:
        reasons.append('capacity_config_mismatch')
    if sample['disk_free_bytes'] < limits['disk_reserve_bytes']:
        reasons.append('capacity_disk_reserve')
    memory = sample.get('memory_bytes')
    if memory is not None and memory > limits['container_memory_total_bytes']:
        reasons.append('capacity_memory')
    return reasons
