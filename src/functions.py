import os
from concurrent.futures import ThreadPoolExecutor

# Set once per worker process by `init_worker` via the Pool initializer.
# A multiprocessing.Barrier cannot be passed as a pickled map() argument under
# the "spawn" start method; it must be inherited through the initializer.
_barrier = None


def resolve_process_count(requested: int | None) -> int:
    """Default to CPU count; always at least 1."""

    cpu = os.cpu_count() or 1
    procs = requested if requested else cpu

    return max(procs, 1)


def future_thread_executor(
    args: list, threads: int | None = None, override_threads: bool = False
):
    """
    Run a list of callables concurrently on a thread pool and return their
    results in submission order.

    Each element of `args` is a list whose first item is the callable and whose
    remaining items are its positional arguments: [fn, arg1, arg2, ...].

    threads / override_threads control the worker count:
      - default: min(threads, cpu_count * 2), or cpu_count * 2 if threads is None
      - override_threads: use exactly `threads` workers (e.g. one per task)

    Used by the blocking-driver path (Databricks): the connector has no async
    API, so concurrency within a process comes from threads here rather than
    asyncio.gather. Threads spend their time blocked on the network/server, so
    the GIL is not the bottleneck.
    """

    futures_list = []
    results = []
    workers: int = (os.cpu_count() or 1) * 2

    if threads:
        workers = min(threads, workers)

    if override_threads and threads:
        workers = threads

    # If only one worker, run in the calling thread to avoid pool overhead.
    if workers == 1:
        for arg in args:
            results.append(arg[0](*arg[1:]))

        return results

    with ThreadPoolExecutor(max_workers=workers) as executor:
        for arg in args:
            # * unpacks [fn, arg1, ...] into executor.submit(fn, arg1, ...)
            futures_list.append(executor.submit(*arg))

        # Collect in submission order. A task that raises is logged and recorded
        # as None rather than aborting the whole batch, so one failed query does
        # not lose the results of its bucket-mates.
        for future in futures_list:
            try:
                results.append(future.result())
            except Exception as e:
                print(f"Thread task error: {e}")
                results.append(None)

    return results


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
