"""Start/status/stop only this experiment. No daemon trade controls are exposed."""
import json
import os
import subprocess
import sys
from session_common import L,T,RUN,read,save,stop_request,now
from session_identity import initialize,process_identity

def main():
    command=sys.argv[1]if len(sys.argv)>1 else'status'
    if command=='status':print(json.dumps(read(L/'STATUS.json',{'phase':'not_started'}),ensure_ascii=False,indent=2));return
    if command=='stop':stop_request('owner_stop');print('STOP_REQUESTED '+RUN);return
    if command!='start':raise SystemExit('choose start/status/stop')
    scope=read(T/'LAUNCH_SCOPE.json',{})
    if scope.get('authorized')is not True:raise ValueError('fresh_session_owner_scope_required')
    if(L/'STOP_REQUEST.json').exists()or(T/'FINAL_REPORT_RU.md').exists():raise ValueError('completed_session_cannot_restart_as_new_experiment')
    review=read(T/'independent-review/PRESTART_DECISION.json')
    if not review or review.get('ready_for_start')is not True:raise ValueError('independent_prestart_review_required')
    existing=read(L/'GUARDIAN.json',{})
    if existing and process_identity(existing['pid'])==existing['identity']:raise ValueError('task_already_running')
    from session_prestart import check
    check()
    initialize()
    from session_deadline import anchor
    anchor()
    with (L/'logs/guardian.log').open('ab',buffering=0)as out:
        p=subprocess.Popen([sys.executable,'-B',str(T/'helpers/session_guardian.py')],cwd=L,stdin=subprocess.DEVNULL,stdout=out,stderr=out,start_new_session=True)
    save(T/'LAUNCH_DISPATCH.json',{'run_id':RUN,'guardian_pid':p.pid,'dispatched_at':now(),'detached':True})
    print(json.dumps({'guardian_pid':p.pid,'status_path':str(L/'STATUS.json'),'detached':True}))

if __name__=='__main__':
    os.umask(0o077);main()
