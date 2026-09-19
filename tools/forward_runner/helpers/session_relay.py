"""Transparent reconnectable TLS transport. Only the backend can reach the provider."""
import json
import os
from pathlib import Path
import selectors
import socket
import sys
import time

def read(path,default=None):
    try:return json.loads(path.read_text())
    except FileNotFoundError:return default
def save(path,value):
    tmp=path.with_suffix('.tmp');tmp.write_text(json.dumps(value));tmp.chmod(0o600);tmp.replace(path)

class Relay:
    def __init__(self,role,directory,sockpath):
        self.role=role;self.d=directory;self.sockpath=sockpath;self.cfg=read(directory/'settings.json')
        self.state={'run_id':self.cfg['run_id'],'generation':self.cfg['generation'],'role':role,
            'phase':'starting','received_bytes':0,'upstream_received_bytes':0,'upstream_sent_bytes':0,
            'connection_headroom_bytes':0,
            'connections':0,'connect_attempts':0,'failures':0,'lease_read_misses':0,'updated_unix':time.time()}
        self.last_save=0;self.last_progress=time.monotonic();self.last_lease={}
    def lease(self):
        try:self.last_lease=json.loads((self.d/'LEASE.json').read_text())
        except FileNotFoundError:self.state['lease_read_misses']+=1
        return self.last_lease
    def valid(self):
        lease=self.lease()
        return not(self.d/'STOP').exists()and not(self.d/'STOP_REQUEST.json').exists()and lease.get('generation')==self.cfg['generation']and lease.get('expires_unix',0)>time.time()and(not lease.get('deadline_unix')or lease['deadline_unix']>time.time())
    def allowance(self):
        if self.role=='front':return 65536
        lease=self.lease()
        return max(0,lease.get('granted_bytes',0)-self.state['connection_headroom_bytes']-self.state['received_bytes'])
    def retry_after(self,failures):
        delay=min(60,2**failures-1);self.state['retry_not_before_unix']=time.time()+delay
        return time.monotonic()+delay
    def status(self,force=False,**fields):
        self.state.update(fields)
        if force or time.monotonic()-self.last_save>=1:
            self.last_save=time.monotonic();self.state['updated_unix']=time.time()
            save(self.d/(self.role+'-status.json'),self.state)
    def pump(self,left,right,initial=b''):
        buffers={left:bytearray(),right:bytearray(initial)};peer={left:right,right:left};sel=selectors.DefaultSelector()
        for stream in peer:stream.setblocking(False)
        reason='STOP_OR_LEASE'
        try:
            while self.valid():
                self.status();remaining=self.allowance()
                for stream in peer:
                    mask=selectors.EVENT_WRITE if buffers[stream]else 0
                    if remaining>0 and len(buffers[peer[stream]])<65536:mask|=selectors.EVENT_READ
                    try:sel.unregister(stream)
                    except KeyError:pass
                    if mask:sel.register(stream,mask)
                for key,mask in sel.select(.05):
                    stream=key.fileobj
                    if mask&selectors.EVENT_WRITE:
                        n=stream.send(buffers[stream]);del buffers[stream][:n]
                        if self.role=='backend'and stream is right:self.state['upstream_sent_bytes']+=n
                    if mask&selectors.EVENT_READ:
                        size=min(16384,self.allowance(),65536-len(buffers[peer[stream]]))
                        if size<=0:continue
                        data=stream.recv(size)
                        if not data:return 'CLIENT_EOF'if stream is left else'UPSTREAM_EOF'
                        self.state['received_bytes']+=len(data)
                        if self.role=='backend'and stream is right:self.state['upstream_received_bytes']+=len(data)
                        buffers[peer[stream]].extend(data);self.last_progress=time.monotonic()
        finally:
            sel.close()
            for stream in peer:
                try:stream.shutdown(socket.SHUT_RDWR)
                except OSError:pass
                stream.close()
        return reason
    def run(self):
        listener=socket.socket(socket.AF_INET if self.role=='front'else socket.AF_UNIX,socket.SOCK_STREAM)
        listener.settimeout(.2)
        if self.role=='front':listener.bind(('127.0.0.1',self.cfg['port']))
        else:
            if self.sockpath.exists():self.sockpath.unlink()
            listener.bind(str(self.sockpath));self.sockpath.chmod(0o600)
        listener.listen(8);self.status(True,phase='ready');failures=0;retry_not_before=0
        try:
            while not(self.d/'STOP').exists():
                if not self.valid():
                    self.status(phase='waiting_lease');time.sleep(.1);continue
                try:client,_=listener.accept()
                except socket.timeout:continue
                right=None
                try:
                    if self.role=='backend'and time.monotonic()<retry_not_before:
                        client.close();self.status(True,phase='retry_backoff');continue
                    if self.role=='backend'and self.allowance()<1024**2+1:
                        client.close();self.status(True,phase='waiting_credit');time.sleep(.1);continue
                    client.settimeout(1);first=client.recv(1)
                    if not first:client.close();continue
                    if not self.valid():client.close();continue
                    self.state['received_bytes']+=len(first)
                    if self.role=='front':
                        right=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);right.settimeout(3);right.connect(str(self.sockpath))
                    else:
                        if not self.valid():client.close();continue
                        if self.allowance()<1024**2:
                            client.close();self.status(True,phase='waiting_credit');time.sleep(.1);continue
                        # Every upstream attempt reserves its own receive-window/shutdown margin.
                        self.state['connection_headroom_bytes']+=1024**2
                        self.state['connect_attempts']+=1;self.status(True,phase='connecting')
                        addr=socket.getaddrinfo(self.cfg['host'],self.cfg['port'],socket.AF_INET,socket.SOCK_STREAM)[0][4]
                        right=socket.socket(socket.AF_INET,socket.SOCK_STREAM);right.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF,131072)
                        right.settimeout(5);right.connect(addr)
                    self.state['connections']+=1;self.status(True,phase='forwarding')
                    started=time.monotonic();reason=self.pump(client,right,first);client=right=None
                    if time.monotonic()-started>=30:
                        failures=0;retry_not_before=0;self.state['retry_not_before_unix']=0
                    elif self.role=='backend'and reason=='UPSTREAM_EOF':
                        failures=min(6,failures+1);retry_not_before=self.retry_after(failures)
                    self.status(True,phase='ready',last_connection_end=reason)
                except (OSError,ValueError,KeyError)as e:
                    failures=min(6,failures+1);self.state['failures']+=1
                    if self.role=='backend':retry_not_before=self.retry_after(failures)
                    self.status(True,phase='retry_backoff',last_error_type=type(e).__name__)
                finally:
                    for stream in(client,right):
                        if stream is not None:stream.close()
        finally:listener.close();self.status(True,phase='stopped')

if __name__=='__main__':
    os.umask(0o077);Relay(sys.argv[1],Path('/control'),Path('/relay/transport.sock')).run()
