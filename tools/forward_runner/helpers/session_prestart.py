"""Read-only checks for the explicit eight-hour proposal; no budget reservations."""
import json
from session_common import T,L,read,save,now


def check():
    from session_prepare import artifact_binding
    artifact=artifact_binding()
    config=read(T/'config/capacity.disabled.json')
    scope=read(T/'LAUNCH_SCOPE.json',{})
    if scope.get('authorized') is not True or scope.get('duration_seconds')!=28800:
        raise ValueError('eight_hour_owner_scope_required')
    from session_sqlite import ensure_bridge
    import session_docker as docker
    cid=ensure_bridge()
    sample=json.loads(docker.docker(['exec',cid,'/usr/bin/python3','-B','-c',
        'import shutil,json;print(json.dumps({"free":shutil.disk_usage("/state").free}))']))
    result={'at':now(),'linux_free_bytes':sample['free'],
        'minimum_free_bytes':config['resource_proposal']['minimum_initial_free_bytes'],
        'artifact_git_sha':artifact['git_sha'],'new_reservations':0}
    save(T/'evidence/PRESTART_RESOURCES.json',result)
    if sample['free']<result['minimum_free_bytes']:
        raise ValueError('linux_storage_below_eight_hour_headroom')
    return result
