"""Detached owner that restarts a failed supervisor without resetting its data or deadline."""
import fcntl
import os
import subprocess
import sys
import time
from session_common import L,T,RUN,read,save,now,event
from session_identity import process_identity

def run():
    lock=(L/'guardian.lock').open('a+');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    pid=os.getpid();save(L/'GUARDIAN.json',{'run_id':RUN,'pid':pid,'identity':process_identity(pid),'started_at':now()})
    awake=subprocess.Popen(['/usr/bin/caffeinate','-ims','-w',str(pid)],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    save(L/'CAFFEINATE.json',{'pid':awake.pid,'identity':process_identity(awake.pid),'parent_guardian':pid})
    failures=0
    try:
        while not(T/'FINAL_REPORT_RU.md').exists():
            previous=read(L/'SUPERVISOR_PROCESS.json',{})
            if previous and process_identity(previous['pid'])==previous.get('identity'):
                if str(T/'helpers/session_supervisor.py')not in previous['identity']:raise ValueError('supervisor_ownership_unknown')
                save(L/'GUARDIAN_STATUS.json',{'at':now(),'phase':'adopted_existing_supervisor','pid':previous['pid'],'restarts':failures})
                time.sleep(.5);continue
            log=L/'logs/supervisor.log'
            if log.exists()and log.stat().st_size>16*2**20:
                log.replace(log.with_name('supervisor.previous.log'))
            with log.open('ab',buffering=0)as out:
                p=subprocess.Popen([sys.executable,'-B',str(T/'helpers/session_supervisor.py')],cwd=L,stdin=subprocess.DEVNULL,stdout=out,stderr=out,start_new_session=True)
                save(L/'SUPERVISOR_DISPATCH.json',{'pid':p.pid,'dispatched_at':now(),'restart':failures})
                rc=p.wait()
            save(L/'GUARDIAN_STATUS.json',{'at':now(),'supervisor_exit_code':rc,'restarts':failures,'final_report_exists':(T/'FINAL_REPORT_RU.md').exists()})
            if(T/'FINAL_REPORT_RU.md').exists():break
            failures+=1;event('supervisor_restart',exit_code=rc,retry_number=failures)
            time.sleep(min(60,2**min(failures,6)))
    finally:
        if awake.poll()is None:awake.terminate();awake.wait(timeout=5)
        save(L/'GUARDIAN_FINISHED.json',{'at':now(),'restarts':failures});lock.close()

if __name__=='__main__':
    os.umask(0o077);run()
