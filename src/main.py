import argparse
import getpass
import os

from typing import Union

from src.postgres_db import PostgresDB
from src.oracle_db import OracleDB


def read_sql_file(file_path: str) -> str:
    """
    Read the SQL query from a file.

    :param file_path: Path to the SQL file
    :return: SQL query as a string
    """

    with open(file_path, "r") as file:
        sql_query = file.read()

    return sql_query


def read_sql_folder(folder_path: str) -> list[str]:
    """
    Read the SQL query from a folder containing multiple SQL files.

    :param folder_path: Path to the folder containing multiple SQL files
    :return: List of SQL queries as strings
    """

    sql_queries = []

    for file_name in os.listdir(folder_path):
        if file_name.endswith(".sql"):
            file_path = os.path.join(folder_path, file_name)
            sql_query = read_sql_file(file_path)
            sql_queries.append(sql_query)

    return sql_queries


def execute_queries_concurrently(
    db: Union[PostgresDB, OracleDB],
    sql_query: Union[str, list[str]],
    num_instances: int,
    fetch_size: int,
):
    # Get the list of durations for each instance
    durations = db.entry(sql_query, num_instances, fetch_size)
    print(durations)

    # Remove None values from the list
    durations = [duration for duration in durations if duration is not None]

    if len(durations) == 0:
        print("No instances completed successfully")
        return

    for i, duration in enumerate(durations):
        print(f"Instance {i + 1}: Execution time: {duration:.2f} seconds")

    # Print the aggregated execution time and each instance's execution time
    print(f"Aggregated execution time: {sum(durations):.2f} seconds")


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
        help="Number of parallel instances to run",
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
    return parser.parse_args()


def main():
    args = arguments()

    if args.sql_folder:
        sql_query = read_sql_folder(args.sql_folder)
    elif args.sql_file:
        sql_query = read_sql_file(args.sql_file)

    if not sql_query:
        raise ValueError("SQL query is empty")

    if args.database == "postgres":
        db = PostgresDB(args.dsn, args.user, getpass.getpass(prompt="Enter password: "))
    elif args.database == "oracle":
        db = OracleDB(args.dsn, args.user, getpass.getpass(prompt="Enter password: "))
    else:
        raise ValueError(f"Unsupported database type: {args.database}")

    execute_queries_concurrently(db, sql_query, args.instances, args.fetch_size)


if __name__ == "__main__":
    main()
