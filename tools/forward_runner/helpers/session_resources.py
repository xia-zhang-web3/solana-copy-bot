"""Machine-state protection; no global file/log growth limits."""
import re
import shutil
import subprocess
from session_common import L,R,save,now

def sample():
    disk=shutil.disk_usage(R)
    ram=int(subprocess.check_output(['/usr/sbin/sysctl','-n','hw.memsize']).strip())
    result=subprocess.run(['/usr/bin/memory_pressure','-Q'],capture_output=True,text=True,timeout=5)
    match=re.search(r'System-wide memory free percentage:\s*(\d+)%',result.stdout)
    free_pct=int(match[1])if match else None
    # Leave memory-sized disk headroom for swap and SQLite's shutdown/checkpoint work.
    reserve=ram
    status={'at':now(),'disk_free_bytes':disk.free,'disk_reserve_bytes':reserve,'ram_bytes':ram,
        'memory_free_percent':free_pct,'wal_bytes':{p.name:p.stat().st_size for p in (L/'state').glob('*-wal')}}
    status['critical']=disk.free<reserve or(free_pct is not None and free_pct<5)
    save(L/'RESOURCES.json',status);return status
