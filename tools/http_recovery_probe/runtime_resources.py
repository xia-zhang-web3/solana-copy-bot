"""Fixed read-only role ceilings, verified against the causal four-fetch fixture."""
GIB = 1024 ** 3
MIB = 1024 ** 2
# Four16MiB bodies + four bounded1MiB requests + three64KiB metadata facts each.
# Round68.75MiB up to72MiB; the controller's8GiB evidence stop remains unchanged.
ARCHIVE_STOP_HEADROOM_BYTES = 72 * MIB
HTTP_ALLOCATOR_ENV = 'MALLOC_ARENA_MAX=2'
ROLE_LIMITS = {
    'observation-app': {'memory': 3 * GIB, 'nano_cpus': 2_000_000_000},
    'http-backend': {'memory': 768 * MIB, 'nano_cpus': 1_000_000_000},
    'http-front': {'memory': 512 * MIB, 'nano_cpus': 1_000_000_000},
    'stream-backend': {'memory': 256 * MIB, 'nano_cpus': 250_000_000},
    'stream-front': {'memory': 256 * MIB, 'nano_cpus': 250_000_000},
}
