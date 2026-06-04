import os


def resolve_process_count(requested: int | None) -> int:
    """Default to CPU count; always at least 1."""

    cpu = os.cpu_count() or 1
    procs = requested if requested else cpu

    return max(procs, 1)


def build_buckets(
    sql_query: str | list[str], instances: int, processes: int
) -> list[list[str]]:
    queries = [sql_query] if isinstance(sql_query, str) else sql_query

    buckets: list[list[str]] = [[] for _ in range(processes)]

    bucket = 0
    for sql in queries:
        for _ in range(0, instances):
            buckets[bucket].append(sql)
            bucket += 1
            if bucket >= processes:
                bucket = 0

    return [b for b in buckets if b]


def worker(payload):
    """
    Top-level worker so it is picklable by multiprocessing.

    payload = (db_factory, db_kwargs, bucket, fetch_size)
    The DB connection object is constructed inside the process from plain
    kwargs; nothing holding a live connection is ever pickled across.
    """

    db_factory, db_kwargs, bucket, page_size = payload
    db = db_factory(**db_kwargs)

    return db.entry(bucket, page_size)
