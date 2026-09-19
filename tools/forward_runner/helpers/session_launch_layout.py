"""Materialize a fresh disabled handoff, without databases or starts."""
import argparse
import json
from pathlib import Path
import shutil


def create(evidence, runtime):
    packet=Path(__file__).resolve().parents[1]
    context=json.loads((packet/'CONTEXT.json').read_text())
    evidence=Path(evidence).resolve();runtime=Path(runtime).resolve()
    if evidence.exists() or runtime.exists():
        raise ValueError('fresh_layout_requires_absent_directories')
    evidence.mkdir(parents=True,mode=0o700)
    for name in ('state','control','configs','logs','private','install'):
        (runtime/name).mkdir(parents=True,mode=0o700)
    from session_prepare import derived_config,CONFIG_SOURCE
    config_text,_=derived_config(CONFIG_SOURCE.read_text())
    disabled=runtime/'configs/live.disabled.toml'
    disabled.write_text(config_text);disabled.chmod(0o600)
    shutil.copytree(packet/'helpers',evidence/'helpers',ignore=shutil.ignore_patterns('__pycache__'))
    shutil.copytree(packet/'config',evidence/'config')
    for name in ('checks','evidence','docker-cli','private','transport'):
        (evidence/name).mkdir(mode=0o700)
    (evidence/'docker-cli/config.json').write_text('{"auths":{}}\n')
    context.update(runtime=str(runtime),evidence=str(evidence),launch_authorized=False,
                   local_acceptance_packet=str(packet))
    (evidence/'CONTEXT.json').write_text(json.dumps(context,indent=2)+'\n')
    shutil.copy2(packet/'CONTEXT.linux.json',evidence/'CONTEXT.linux.json')
    (evidence/'LAUNCH_SCOPE.json').write_text(json.dumps({
        'authorized':False,'duration_seconds':28800,'execution_enabled':False,
        'tiny_submit_enabled':False,'activation':False,'new_reservation_usd':'0',
        'financial_decision_required':'8h with 4x headroom exceeds remaining budget',
        'disk_decision_required':'147 GiB initial Linux free space required for 8h proposal'},indent=2)+'\n')
    return {'evidence':str(evidence),'runtime':str(runtime),'disabled':True,
            'databases_created':0,'provider_requests':0,'source':context['source']}


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--evidence',required=True)
    parser.add_argument('--runtime',required=True)
    args=parser.parse_args()
    print(json.dumps(create(args.evidence,args.runtime)))
