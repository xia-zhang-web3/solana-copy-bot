"""Autonomous eight-hour lifecycle; run under the detached task guardian."""
import os
import fcntl
import signal
import threading
import time
from session_common import L,T,RUN,now,epoch,read,save,event,stopped,stop_request
from session_budget import Budget,Exhausted
from session_http import Broker
from session_identity import verify,stop_operators,operator_callback,process_identity
from session_rpc_server import start_server
from session_resources import sample
from session_workers import start_workers
from session_runtime import LinuxRuntime
from session_deadline import deadline
import session_docker as docker
from session_schedule import schedule as get_schedule

def record_failure(exc,stage):
    import traceback
    details={'at':now(),'stage':stage,'error_type':type(exc).__name__,
        'sqlite_errorcode':getattr(exc,'sqlite_errorcode',None),
        'sqlite_errorname':getattr(exc,'sqlite_errorname',None),
        'frames':[{'file':frame.filename,'function':frame.name,'line':frame.lineno}
                  for frame in traceback.extract_tb(exc.__traceback__)[-12:]]}
    try:
        save(L/'SUPERVISOR_FAILURE.json',details)
        save(T/'checks'/('supervisor-failure-'+str(time.time_ns())+'.json'),details)
    except Exception:
        pass  # Diagnostic I/O must not replace the original exception or stop cleanup.

class Supervisor:
    def __init__(self):
        self.stop=threading.Event();self.ledger=None;self.server=None;self.workers={};self.group=None;self.reason=None
        self.last_check=0;self.last_resource=0;self.restart_count=read(L/'RESTART_COUNTER.json',{}).get('count',0)
    def guard_active(self):
        schedule=get_schedule()
        if stopped():self.reason=read(L/'STOP_REQUEST.json')['reason'];raise InterruptedError(self.reason)
        if self.stop.is_set():raise InterruptedError('worker_stopped')
        if time.time()>=deadline():self.reason='eight_hour_deadline';raise InterruptedError(self.reason)
    def backoff(self,seconds):
        until=time.monotonic()+seconds
        while time.monotonic()<until:
            self.guard_active();self.stop.wait(min(.1,until-time.monotonic()))
    def lease(self):
        self.guard_active()
        if not self.group:return
        status=read(L/'control/backend-status.json',{})
        consumed=(status.get('received_bytes',0)+status.get('connection_headroom_bytes',0))if status.get('generation')==self.group['generation']else 0
        granted=self.budget.stream_grant(self.group['generation'],consumed)
        schedule=get_schedule()
        save(L/'control/LEASE.json',{'run_id':RUN,'generation':self.group['generation'],'expires_unix':time.time()+15,
            'granted_bytes':granted,'deadline_unix':deadline()})
    def stop_containers(self):
        (L/'control/STOP').touch(mode=0o600,exist_ok=True)
        results=[]
        for group in docker.registry()['generations']:
            for role in('app','front','backend'):
                cid=group.get('cids',{}).get(role)
                if not cid:continue
                row=docker.registry()['containers'][cid]
                if row.get('removed'):continue
                obj=docker.stop_owned(cid);results.append({'role':role,'generation':group['generation'],'id':cid,'state':obj['State']})
                status=read(L/'control'/f'{role}-status.json',{})
                if status.get('generation')==group['generation']:
                    save(T/'transport'/f"generation-{group['generation']}-{role}.json",status)
        save(L/'CONTAINER_STOP_RESULTS.json',results);return results
    def start_generation(self):
        self.guard_active()
        self.stop_containers()
        self.guard_active()
        generation=max([g['generation']for g in docker.registry()['generations']]+[0])+1
        self.group=docker.create_generation(generation)
        settings=read(L/'control/settings.json');settings['generation']=generation;save(L/'control/settings.json',settings)
        (L/'control/STOP').unlink(missing_ok=True);self.lease()
        for role in('backend','front','app'):
            self.guard_active();self.lease();docker.start_owned(self.group['cids'][role])
            if role!='app':
                until=time.monotonic()+10
                while time.monotonic()<until:
                    status=read(L/'control'/f'{role}-status.json',{})
                    if status.get('generation')==generation and status.get('phase')in('ready','waiting_lease'):break
                    time.sleep(.1)
                else:raise RuntimeError('relay_not_ready_'+role)
        event('runtime_generation_started',generation=generation,cids=self.group['cids'])
    def state(self):
        save(L/'RESTART_COUNTER.json',{'count':self.restart_count,'updated_at':now()})
        schedule=get_schedule();membership=read(L/'MEMBERSHIP.json',{});d=membership.get('decision',{})
        phase='awaiting_fresh_ingress'if not schedule else'collecting_with_membership'if membership.get('effective',{}).get('status')=='ACKED'and d.get('admissible')and epoch(d['valid_until'])>time.time()else'warming_or_closed_admissions'
        status={'run_id':RUN,'updated_at':now(),'supervisor_pid':os.getpid(),'phase':phase,'schedule':schedule,
            'budget':self.budget.summary(),'discovery':read(L/'DISCOVERY_SCHEDULER.json'),
            'membership':{'decision_id':d.get('decision_id'),'admissible_now':phase=='collecting_with_membership','wallets':d.get('wallets',[]),'valid_until':d.get('valid_until')},
            'observer':read(L/'OBSERVER.json'),'virtual':self.ledger.summary(),
            'workers':{k:t.is_alive()for k,t in self.workers.items()},'generation':self.group['generation']if self.group else None,
            'relay':read(L/'control/backend-status.json'),'restart_count':self.restart_count,'execution_enabled':False,
            'tiny_submit_enabled':False,'activation':False,'trades':0,'future_report':str(T/'FINAL_REPORT_RU.md')}
        save(L/'STATUS.json',status);save(T/'STATUS.json',status)
    def step(self):
        if stopped():self.reason=read(L/'STOP_REQUEST.json')['reason'];return False
        schedule=get_schedule()
        if time.time()>=deadline():self.reason='eight_hour_deadline';return False
        verify();self.lease()
        if time.monotonic()-self.last_resource>=30:
            self.last_resource=time.monotonic()
            if sample()['critical']:self.reason='machine_resource_pressure';return False
            try:
                from session_capacity import validate_sample,tree_bytes
                capacity=self.ledger.capacity()
                host_state,host_wal=tree_bytes(L/'state')
                capacity['state_physical_bytes']+=host_state
                capacity['wal_bytes']+=host_wal
                retained_http=tree_bytes(T/'private')[0] if (T/'private').is_dir() else 0
                app_generations=sum(r['role']=='app' and not r.get('removed') for r in docker.registry()['containers'].values())
                capacity['logs_bytes']=tree_bytes(L/'logs')[0]+retained_http+app_generations*64*2**20
                capacity['logs_basis']='host logs + retained HTTP evidence + 64MiB cap per retained app generation'
                reasons=validate_sample(capacity,read(T/'config/capacity.disabled.json'))
                save(L/'CAPACITY.json',{'sample':capacity,'stop_reasons':reasons,'at':now()})
                if reasons:self.reason=','.join(reasons);return False
            except Exception as exc:
                self.reason='capacity_sample_failed_'+type(exc).__name__;return False
        if time.monotonic()-self.last_check>=10:
            self.last_check=time.monotonic()
            bad=[role for role,cid in self.group['cids'].items()if not docker.inspect_owned(cid)['State']['Running']]
            if bad:
                event('owned_process_recovery',roles=bad);self.restart_count+=1
                self.backoff(min(60,2**min(self.restart_count,6)));self.start_generation()
        self.state();return True
    def run(self):
        verify(full=True);stop_operators();self.budget=Budget();self.broker=Broker(self.budget)
        try:
            self.ledger=LinuxRuntime(self.broker);self.server,endpoint=start_server(self.broker)
            self.workers=start_workers(self.ledger,self.broker,endpoint,self.stop,operator_callback,remote_jobs=True)
            while not self.stop.is_set():
                if stopped():self.reason=read(L/'STOP_REQUEST.json')['reason'];break
                schedule=get_schedule()
                if time.time()>=deadline():self.reason='eight_hour_deadline';break
                try:
                    if not self.group:self.start_generation()
                    if not self.step():break
                    self.stop.wait(1)
                except RuntimeError as e:
                    event('docker_or_control_retry',error_type=type(e).__name__)
                    self.restart_count+=1;self.group=None;self.backoff(min(60,2**min(self.restart_count,6)))
            self.reason=self.reason or read(L/'STOP_REQUEST.json',{}).get('reason','worker_stopped')
        except Exhausted:self.reason='budget_exhausted'
        except InterruptedError:self.reason=self.reason or 'worker_stopped'
        except (ValueError,__import__('sqlite3').DatabaseError)as e:
            self.reason='integrity_'+type(e).__name__;event('supervisor_integrity_failure',error_type=type(e).__name__)
        except Exception as e:
            record_failure(e,'run_before_cleanup');raise
        finally:self.finish()
    def finish(self):
        self.reason=self.reason or 'supervisor_control_failure';stop_request(self.reason);self.stop.set()
        stopped_rows=[];failures=[]
        try:
            if self.ledger:self.ledger.stop_pending(self.reason)
        except Exception as e:failures.append('controller_stop_'+type(e).__name__)
        try:stopped_rows=self.stop_containers()
        except Exception as e:failures.append(type(e).__name__)
        for thread in self.workers.values():thread.join(timeout=8)
        try:stop_operators()
        except Exception as e:failures.append(type(e).__name__)
        ledger_export=None
        try:
            if self.ledger:ledger_export=self.ledger.export()
        except Exception as e:failures.append('controller_export_'+type(e).__name__)
        try:
            if self.ledger:self.ledger.close()
        except Exception as e:failures.append('controller_close_'+type(e).__name__)
        try:
            from session_sqlite import stop_bridge
            bridge_stop=stop_bridge()
        except Exception as e:
            bridge_stop=None;failures.append(type(e).__name__)
        if self.server:self.server.shutdown();self.server.server_close()
        from session_report import generate_final
        evidence={'at':now(),'reason':self.reason,'containers':stopped_rows,'stop_failures':failures,
            'workers_alive_after_stop':{k:t.is_alive()for k,t in self.workers.items()},'sqlite_bridge':bridge_stop}
        save(L/'STOPPED.json',evidence)
        generate_final(self.reason,evidence,ledger_export,self.budget.summary()if hasattr(self,'budget')else None)
        final={'run_id':RUN,'phase':'completed','completed_at':now(),'stop_reason':self.reason,'schedule':read(L/'SCHEDULE.json'),
            'future_report':str(T/'FINAL_REPORT_RU.md'),'stop_evidence':str(L/'STOPPED.json'),'execution_enabled':False,'trades':0}
        save(L/'STATUS.json',final);save(T/'STATUS.json',final)

if __name__=='__main__':
    os.umask(0o077)
    lock=(L/'supervisor.lock').open('a+')
    try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:raise SystemExit(75)
    save(L/'SUPERVISOR_PROCESS.json',{'pid':os.getpid(),'identity':process_identity(os.getpid()),'started_at':now()})
    supervisor=Supervisor()
    signal.signal(signal.SIGTERM,lambda *_:stop_request('supervisor_sigterm'))
    try:supervisor.run()
    except (ValueError,__import__('sqlite3').DatabaseError)as e:
        supervisor.reason='integrity_'+type(e).__name__;supervisor.finish()
    except Exception as e:
        event('supervisor_unhandled',error_type=type(e).__name__)
        raise SystemExit(1)
