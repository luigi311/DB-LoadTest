import os

# Set once per worker process by `init_worker` via the Pool initializer.
# A multiprocessing.Barrier cannot be passed as a pickled map() argument under
# the "spawn" start method; it must be inherited through the initializer.
_barrier = None


def resolve_process_count(requested: int | None) -> int:
    """Default to CPU count; always at least 1."""

    cpu = os.cpu_count() or 1
    procs = requested if requested else cpu

    return max(procs, 1)


def build_buckets(
    queries: list[tuple[str, str]], instances: int, processes: int
) -> list[list[tuple[str, str]]]:
    buckets: list[list[tuple[str, str]]] = [[] for _ in range(processes)]

    bucket = 0
    for item in queries:
        for _ in range(0, instances):
            buckets[bucket].append(item)
            bucket += 1
            if bucket >= processes:
                bucket = 0

    return [b for b in buckets if b]


def merge_durations(
    target: dict[str, list[float]], source: dict[str, list[float]]
) -> None:
    """Merge a worker's {file_name: [durations]} dict into the running total."""

    for file_name, durations in source.items():
        target.setdefault(file_name, []).extend(durations)


def init_worker(barrier):
    """
    Pool initializer: runs once per worker as it starts up. Stashes the shared
    barrier in a module global so `worker` can reach it without it being passed
    through map() (which would fail to pickle under the spawn start method).
    """

    global _barrier
    _barrier = barrier


def worker(payload):
    """
    Top-level worker so it is picklable by multiprocessing.

    payload = (db_factory, db_kwargs, bucket)
    The DB connection object is constructed inside the process from plain
    kwargs; nothing holding a live connection is ever pickled across.

    Before doing any DB work the worker waits on the shared barrier so every
    process has finished spawning. All workers then leave the barrier together
    and begin connecting/executing at the same moment, removing the staggered
    interpreter-startup time from the run.

    Returns {file_name: [durations]} for the queries in this bucket.
    """

    db_factory, db_kwargs, bucket = payload
    db = db_factory(**db_kwargs)

    # Wait until every worker has spawned and reached this point, then release
    # together. The slowest process to start is what kicks off the whole run.
    if _barrier is not None:
        _barrier.wait()

    return db.entry(bucket)
