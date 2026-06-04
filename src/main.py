import argparse
import getpass
import multiprocessing
import os

from src.functions import (
    build_buckets,
    init_worker,
    merge_durations,
    resolve_process_count,
    worker,
)
from src.oracle_db import OracleDB
from src.postgres_db import PostgresDB


def read_sql_file(file_path: str) -> tuple[str, str]:
    """
    Read the SQL query from a file.

    :param file_path: Path to the SQL file
    :return: (file_name, sql_query) pair; file_name is the basename
    """

    with open(file_path, "r") as file:
        sql_query = file.read()

    return os.path.basename(file_path), sql_query


def read_sql_folder(folder_path: str) -> list[tuple[str, str]]:
    """
    Read the SQL queries from a folder containing multiple SQL files.

    :param folder_path: Path to the folder containing multiple SQL files
    :return: List of (file_name, sql_query) pairs
    """

    queries = []

    for file_name in os.listdir(folder_path):
        if file_name.endswith(".sql"):
            file_path = os.path.join(folder_path, file_name)
            queries.append(read_sql_file(file_path))

    return queries


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run an SQL query on an Oracle or PostgreSQL database."
    )
    parser.add_argument(
        "--dsn", required=True, help="Data Source Name (DSN) for the database"
    )
    parser.add_argument("--user", required=True, help="Username for the database")
    parser.add_argument("--sql_file", help="Path to the SQL file")
    parser.add_argument(
        "--sql_folder", help="Path to the folder containing multiple SQL files"
    )
    parser.add_argument(
        "--instances",
        type=int,
        required=True,
        help="Number of parallel instances (copies) to run per query",
    )
    parser.add_argument(
        "--processes",
        type=int,
        default=0,
        help="Number of worker processes to spread the load across "
        "(default 0 = CPU count)",
    )
    parser.add_argument(
        "--fetch_size",
        type=int,
        default=0,
        help="Number of rows to fetch per batch (default 0, no fetch)",
    )
    parser.add_argument(
        "--database",
        default="postgres",
        choices=["postgres", "oracle"],
        help="Database type (default postgres)",
    )
    parser.add_argument(
        "--print",
        action="store_true",
        help="Print the results of the SQL query (default False)",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default="/*DB-LoadTest*/",
        help="Text to prefix the sql statements with so they can be easily "
        "identified in the database",
    )
    return parser.parse_args()


def report(results: dict[str, list[float]]) -> None:
    """Print per-file stats plus an overall roll-up."""

    if not results:
        print("No instances completed successfully")
        return

    all_durations: list[float] = []
    for file_name in sorted(results):
        durations = results[file_name]
        all_durations.extend(durations)

        print(f"\n{file_name}")
        print(f"  Runs      : {len(durations)}")
        print(f"  Aggregated: {sum(durations):.2f}s")
        print(f"  Average   : {sum(durations) / len(durations):.2f}s")
        print(f"  Minimum   : {min(durations):.2f}s")
        print(f"  Maximum   : {max(durations):.2f}s")

    print(f"\n{'=' * 40}")
    print(f"Overall runs      : {len(all_durations)}")
    print(f"Overall aggregated: {sum(all_durations):.2f}s")
    print(f"Overall average   : {sum(all_durations) / len(all_durations):.2f}s")
    print(f"Overall minimum   : {min(all_durations):.2f}s")
    print(f"Overall maximum   : {max(all_durations):.2f}s")
    print(f"{'=' * 40}")


def execute_queries_concurrently(
    db_factory,
    db_kwargs: dict,
    buckets: list[list[tuple[str, str]]],
    fetch_size: int,
):
    payloads = [(db_factory, db_kwargs, bucket, fetch_size) for bucket in buckets]

    # Accumulate {file_name: [durations]} across all workers.
    results: dict[str, list[float]] = {}

    if len(buckets) == 1:
        # Single worker: nothing to synchronize, run inline without a barrier.
        db = db_factory(**db_kwargs)
        merge_durations(results, db.entry(buckets[0], fetch_size))
    else:
        ctx = multiprocessing.get_context("spawn")
        # Barrier sized to the worker count. Each worker waits here once it has
        # spawned and built its DB object, so all processes leave together and
        # begin connecting/executing at the same moment -- removing the
        # staggered interpreter-startup time from the measured run.
        barrier = ctx.Barrier(len(buckets))
        with ctx.Pool(
            processes=len(buckets),
            initializer=init_worker,
            initargs=(barrier,),
        ) as pool:
            for result in pool.map(worker, payloads):
                merge_durations(results, result)

    report(results)


def main():
    args = arguments()

    queries: list[tuple[str, str]] = []
    if args.sql_folder:
        queries = read_sql_folder(args.sql_folder)
    elif args.sql_file:
        queries = [read_sql_file(args.sql_file)]

    if not queries:
        raise ValueError("SQL query is empty")

    processes = resolve_process_count(args.processes)
    buckets = build_buckets(queries, args.instances, processes)

    db_factory = PostgresDB if args.database == "postgres" else OracleDB
    db_kwargs = {
        "dsn": args.dsn,
        "user": args.user,
        "password": getpass.getpass(prompt="Enter password: "),
        "printing": args.print,
        "prefix": args.prefix,
    }

    execute_queries_concurrently(db_factory, db_kwargs, buckets, args.fetch_size)


if __name__ == "__main__":
    main()
