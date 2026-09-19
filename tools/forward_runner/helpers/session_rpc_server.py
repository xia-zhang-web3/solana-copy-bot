"""Private loopback-only RPC bridge for native Discovery portfolio checks."""
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
import secrets
import threading
import uuid
from session_common import L,save

def start_server(broker):
    token=secrets.token_hex(24);route='/rpc/'+token
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_POST(self):
            if self.path!=route:self.send_error(404);return
            try:
                request=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                if request.get('method')not in ('getBalance','getTokenAccountsByOwner'):raise ValueError('portfolio_method_only')
                a=broker.once('alchemy',request['method'],request['params'],'discovery:'+uuid.uuid4().hex)
                body=a.get('body')
                if not isinstance(body,dict):body={'jsonrpc':'2.0','error':{'code':-32000,'message':a['status']}}
                body['id']=request.get('id');raw=json.dumps(body).encode()
                self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
            except (BrokenPipeError,ConnectionResetError):pass
            except Exception as e:
                raw=json.dumps({'jsonrpc':'2.0','id':None,'error':{'code':-32000,'message':type(e).__name__}}).encode()
                try:self.send_response(503);self.end_headers();self.wfile.write(raw)
                except (BrokenPipeError,ConnectionResetError):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.daemon_threads=True
    endpoint=f'http://127.0.0.1:{server.server_port}{route}'
    save(L/'configs/BROKER_PRIVATE.json',{'endpoint':endpoint})
    threading.Thread(target=server.serve_forever,daemon=True,name='portfolio-rpc').start()
    return server,endpoint
