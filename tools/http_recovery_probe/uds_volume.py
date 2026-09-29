"""Prepare only the new task-owned UDS volume; no provider or daemon can start."""
import json
from pathlib import Path
import re
import subprocess

IMAGE = 'sha256:09ecaa87c6799c8d8ee0dfb779905d97e5f667ab97d866cc47ff9f949ffb7b3b'
LABEL = 'copybot.http-recovery-probe'
ROLE = 'copybot.resource-role'
INITIALIZE = '''import json,os,stat
p="/relay";s=os.stat(p)
identity=(s.st_uid,s.st_gid,stat.S_IMODE(s.st_mode))
before={"uid":s.st_uid,"gid":s.st_gid,"mode":oct(identity[2]),"entries":None}
if identity==(0,0,0o755):
 before["entries"]=len(os.listdir(p))
 if before["entries"]: raise ValueError("nonempty_owned_volume_refused")
 os.chmod(p,0o700);os.chown(p,501,20)
elif identity!=(501,20,0o700): raise ValueError("unexpected_owned_volume_permissions")
s=os.stat(p)
print(json.dumps({"before":before,"after":{"uid":s.st_uid,"gid":s.st_gid,"mode":oct(stat.S_IMODE(s.st_mode))}}))
'''
BIND = '''import json,os,socket,stat
p="/relay";s=os.stat(p)
if (s.st_uid,s.st_gid,stat.S_IMODE(s.st_mode))!=(501,20,0o700): raise ValueError("uds_owner_mode")
if os.getuid()!=501 or os.getgid()!=20 or os.listdir(p): raise ValueError("uds_fixture_identity_or_content")
q=p+"/proof.sock";listener=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
try:
 listener.bind(q);listener.listen(1)
 print(json.dumps({"unix_listener_bind":"PASS","uid":os.getuid(),"gid":os.getgid(),"network":"none","provider_calls":0}))
finally:
 listener.close()
 if os.path.exists(q):os.unlink(q)
'''


class PreparationFailure(ValueError):
    def __init__(self, action, result):
        self.fact = dict(action=action, exit_code=result.returncode,
                         stdout=result.stdout[-4096:], stderr=result.stderr[-4096:])
        super().__init__('uds_preparation_' + action + '_failed')


def invoke(arguments):
    return subprocess.run(['docker', *arguments], capture_output=True, text=True, timeout=30)


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    path.chmod(0o600)


def prepare_uds(run_id, evidence, runner=None):
    if not re.fullmatch(r'copybot-run15-http-recovery-probe-[0-9]{2,}', run_id):
        raise ValueError('uds_exact_task_run_required')
    evidence = Path(evidence)
    if not evidence.is_dir():
        raise ValueError('uds_task_evidence_directory_required')
    runner = runner or invoke
    volume = run_id + '-uds'
    expected_labels = {LABEL: run_id, ROLE: 'uds'}

    def run(action, *args):
        result = runner(list(args))
        if result.returncode:
            raise PreparationFailure(action, result)
        return result.stdout.strip()

    try:
        image = json.loads(run('image_identity', 'image', 'inspect', IMAGE))[0]
        if (image['Id'], image['Os'], image['Architecture']) != (IMAGE, 'linux', 'amd64'):
            raise ValueError('uds_cached_image_identity')
        if run('volume_users', 'ps', '-aq', '--filter', 'volume=' + volume):
            raise ValueError('uds_volume_already_attached')
        exists = runner(['volume', 'inspect', volume])
        if exists.returncode == 0:
            current = json.loads(exists.stdout)[0]
            if (current['Name'] != volume or current['Driver'] != 'local'
                    or current.get('Options') or current.get('Labels') != expected_labels):
                raise ValueError('uds_volume_ownership_or_isolation')
        elif exists.returncode == 1 and 'no such volume' in exists.stderr.lower() and volume in exists.stderr:
            run('volume_create', 'volume', 'create', '--driver', 'local',
                '--label', LABEL + '=' + run_id, '--label', ROLE + '=uds', volume)
        else:
            raise PreparationFailure('volume_inspect', exists)
        current = json.loads(run('volume_identity', 'volume', 'inspect', volume))[0]
        if (current['Name'] != volume or current['Driver'] != 'local' or current.get('Options')
                or current.get('Labels') != expected_labels):
            raise ValueError('uds_created_volume_identity')
        if run('volume_users', 'ps', '-aq', '--filter', 'volume=' + volume):
            raise ValueError('uds_volume_already_attached')
        common = ['run', '--rm', '--platform', 'linux/amd64', '--pull', 'never', '--network', 'none',
                  '--memory', '64m', '--memory-swap', '64m', '--cpus', '1', '--pids-limit', '16',
                  '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
                  '--label', LABEL + '=' + run_id, '--mount', 'type=volume,src=' + volume + ',dst=/relay']
        ownership = json.loads(run('initialize', *common, '--cap-add', 'CHOWN',
                                   '--entrypoint', '/usr/bin/python3', IMAGE, '-B', '-c', INITIALIZE))
        if ownership['after'] != dict(uid=501, gid=20, mode='0o700'):
            raise ValueError('uds_owner_mode_not_established')
        bind = json.loads(run('unix_bind', *common, '--user', '501:20',
                             '--entrypoint', '/usr/bin/python3', IMAGE, '-B', '-c', BIND))
        if bind != dict(unix_listener_bind='PASS', uid=501, gid=20, network='none', provider_calls=0):
            raise ValueError('uds_actual_unix_bind_not_proven')
        result = dict(status='PASS_FRESH_OWNED_UDS', run_id=run_id, volume=volume, image=IMAGE,
                      labels=expected_labels, ownership=ownership, unix_bind=bind,
                      provider_roles_created=0, provider_calls=0, signatures=0, submissions=0)
        save(evidence / 'UDS_VOLUME_BINDING.json', result)
        return result
    except PreparationFailure as error:
        save(evidence / 'UDS_VOLUME_FAILURE.json', error.fact)
        raise
