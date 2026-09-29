"""One bounded read-only block contract, with legacy route limits unchanged."""
RECOVERY_PROFILE = 'read_only_http_recovery_v1'
DEFAULT_BODY_BYTES = 4_194_304
LEGACY_BLOCK_BYTES = 8_388_608
RECOVERY_BLOCK_BYTES = 16_777_216
DEFAULT_FRAME_BYTES = 8_500_000
LEGACY_BLOCK_FRAME_BYTES = 12_000_000
FRAME_ENVELOPE_BYTES = 65_536
FORWARDED_HEADER_BYTES = 1_024  # Two headers; JSON escaping still fits the envelope.
RECOVERY_BASE64_BYTES = 4*((RECOVERY_BLOCK_BYTES+2)//3)
RECOVERY_FRAME_BYTES = RECOVERY_BASE64_BYTES+FRAME_ENVELOPE_BYTES
LAYERS = {'upstream_declared', 'upstream_body', 'upstream_header', 'archive', 'frame', 'front_body'}
OBSERVATIONS = {'declared', 'exact', 'lower_bound'}


def response_limit(route, method, profile=None):
    if (route, method) != ('rpc', 'getBlock'):
        return DEFAULT_BODY_BYTES
    return RECOVERY_BLOCK_BYTES if profile == RECOVERY_PROFILE else LEGACY_BLOCK_BYTES


def frame_limit(route, method, profile=None):
    if (route, method) != ('rpc', 'getBlock'):
        return DEFAULT_FRAME_BYTES
    return RECOVERY_FRAME_BYTES if profile == RECOVERY_PROFILE else LEGACY_BLOCK_FRAME_BYTES


def size_error(reason, layer, observed, limit, observation='exact'):
    error = ValueError(reason)
    error.size_fact = dict(size_layer=layer, observed_bytes=observed,
                          size_limit_bytes=limit, size_observation=observation)
    return error


def check_size(size, limit, layer, observation='exact', reason='upstream_response_too_large'):
    if size > limit:
        raise size_error(reason, layer, size, limit, observation)


def check_headers(headers, profile, route='rpc', method='getBlock'):
    if profile == RECOVERY_PROFILE and (route, method) == ('rpc', 'getBlock'):
        for value in headers.values():
            check_size(len(value.encode()), FORWARDED_HEADER_BYTES, 'upstream_header')
    return headers


def safe_size_fact(value):
    if not isinstance(value, dict):
        return {}
    fact = {}
    for key, allowed in [('size_layer', LAYERS), ('size_observation', OBSERVATIONS)]:
        if value.get(key) in allowed:
            fact[key] = value[key]
    for key in ('observed_bytes', 'size_limit_bytes'):
        if type(value.get(key)) is int and 0 <= value[key] <= 2**53:
            fact[key] = value[key]
    return fact
