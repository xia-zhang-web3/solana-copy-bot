"""Terminal probe evidence survives optional Docker/log/reader failures."""
import probe_once as controller


def finalize(cids, result, primary_failure, restarts):
    root = controller.ROOT
    stop_errors = controller.stop(cids)
    diagnostic_errors = []
    def optional(path, stage):
        try:
            return controller.read(path)
        except Exception as error:
            diagnostic_errors.append(dict(stage=stage, error_type=type(error).__name__, reason=str(error)[:160]))
            return None
    backend = optional(root / 'control/backend-status.json', 'backend_metrics')
    if backend is None:
        diagnostic_errors.append(dict(stage='backend_metrics', error='MISSING'))
    try:
        latest = controller.observation(cids['observation-app'])
    except Exception as error:
        diagnostic_errors.append(dict(stage='app_logs', error_type=type(error).__name__, reason=str(error)[:160]))
        latest = dict(status='UNKNOWN', transport=None, ingress=None)
    try:
        counts = controller.financial_counts()
    except Exception as error:
        diagnostic_errors.append(dict(stage='financial_reader', error_type=type(error).__name__, reason=str(error)[:160]))
        counts = dict(status='UNKNOWN', orders=None, positions=None, execution_canary_receipt_facts=None)
    budget = None
    try:
        if backend is not None and backend.get('phase') == 'stopped':
            budget = controller.ledger(backend)
        else:
            budget = controller.unresolved_ledger(backend)
    except Exception as error:
        diagnostic_errors.append(dict(stage='ledger', error_type=type(error).__name__, reason=str(error)[:160]))
        if isinstance(error, ValueError) and str(error) == 'probe_metrics_unknown':
            try:
                budget = controller.unresolved_ledger(backend)
            except Exception as fallback_error:
                diagnostic_errors.append(dict(stage='ledger_reservation', error_type=type(fallback_error).__name__,
                                              reason=str(fallback_error)[:160]))
    stopped = {}
    for role, cid in cids.items():
        try:
            stopped[role] = not controller.verify_container(cid)['State']['Running']
        except Exception as error:
            stopped[role] = None
            diagnostic_errors.append(dict(stage='container_state', role=role,
                error_type=type(error).__name__, reason=str(error)[:160]))
    value = dict(run_id=controller.RUN, result=result, primary_failure=primary_failure, stop_errors=stop_errors,
        diagnostic_errors=diagnostic_errors, ledger=budget, financial=counts, observation=latest,
        app_restarts=restarts, stream_deadline=optional(root / 'control/PROBE_CLOCK.json', 'clock'),
        financial_stop_preserved=(root / 'control/STOP').is_file(), signatures=0, submissions=0,
        http_rpc_requests=0, additional_rpc_cu=0, containers_stopped=all(v is True for v in stopped.values()),
        container_stopped_states=stopped)
    controller.save(root / 'control/RESULT.json', value)
    controller.save(root / 'evidence/RESULT.json', value)
    return value
