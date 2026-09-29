"""Offline-only role memory/barrier witnesses; no production policy changes."""
import gc
import json
from pathlib import Path
import resource
import threading
import time


def save(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, sort_keys=True))
    temporary.replace(path)


def resident():
    fields = {}
    for line in Path('/proc/self/smaps_rollup').read_text().splitlines():
        words = line.split()
        if words and words[0] in {'Rss:', 'Pss:'}:
            fields[words[0][:-1].lower() + '_bytes'] = int(words[1]) * 1024
    for name in ['memory.current', 'memory.peak', 'memory.max', 'memory.swap.current']:
        fields[name] = int(Path('/sys/fs/cgroup/' + name).read_text())
    fields['maxrss_bytes'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
    return fields


class Metrics:
    def __init__(self, directory, role):
        self.directory, self.role = Path(directory), role
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.samples, self.stages = [], []
        self.barriers = {stage: threading.Barrier(4, timeout=7)
                         for stage in ['upstream', 'archive', 'frame', 'front']}
        self.active, self.maximum = {}, {}
        self.thread = threading.Thread(target=self.observe, daemon=True)
        self.thread.start()

    def observe(self):
        while not self.stop.wait(.05):
            with self.lock:
                self.samples.append(dict(at_unix=time.time(), **resident()))

    def hold(self, stage, byte_count):
        if not (self.directory / 'PARALLEL_MODE').exists():
            return
        with self.lock:
            self.active[stage] = self.active.get(stage, 0) + 1
            self.maximum[stage] = max(self.maximum.get(stage, 0), self.active[stage])
            self.stages.append(dict(stage=stage, bytes=byte_count,
                                    active=self.active[stage], **resident()))
        try:
            self.barriers[stage].wait()
            time.sleep(.10)  # Four actual bodies/frames remain resident for sampling.
        finally:
            with self.lock:
                self.active[stage] -= 1

    def snapshot(self, label):
        gc.collect()
        with self.lock:
            self.stages.append(dict(stage=label, **resident()))
        self.flush()

    def flush(self):
        with self.lock:
            save(self.directory / (self.role + '-memory.json'), dict(
                role=self.role, samples=self.samples, stages=self.stages,
                maximum_parallel=self.maximum, final=resident()))

    def close(self):
        self.stop.set()
        self.thread.join(timeout=1)
        self.flush()
