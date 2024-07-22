import argparse
import getpass


def read_sql_file(file_path):
    """
    Read the SQL query from a file.

    :param file_path: Path to the SQL file
    :return: SQL query as a string
    """

    with open(file_path, "r") as file:
        sql_query = file.read()

    return sql_query


def execute_queries_concurrently(db, sql_query, num_instances, fetch_size):
    # Get the list of durations for each instance
    durations = db.entry(sql_query, num_instances, fetch_size)
    print(durations)

    # Remove None values from the list
    durations = [duration for duration in durations if duration is not None]

    if len(durations) == 0:
        print("No instances completed successfully")
        return

    # Print the total execution time and each instance's execution time
    print(f"Total execution time: {sum(durations):.2f} seconds")

    for i, duration in enumerate(durations):
        print(f"Instance {i + 1}: Execution time: {duration:.2f} seconds")


def arguments():
    parser = argparse.ArgumentParser(
        description="Run an SQL query on an Oracle or PostgreSQL database."
    )
    parser.add_argument(
        "--dsn", required=True, help="Data Source Name (DSN) for the database"
    )
    parser.add_argument("--user", required=True, help="Username for the database")
    parser.add_argument("--sql_file", required=True, help="Path to the SQL file")
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

    sql_query = read_sql_file(args.sql_file)

    if not sql_query:
        raise ValueError("SQL query is empty")

    if args.database == "postgres":
        from src.postgres_db import PostgresDB

        db = PostgresDB(args.dsn, args.user, getpass.getpass(prompt="Enter password: "))

    elif args.database == "oracle":
        from src.oracle_db import OracleDB

        db = OracleDB(args.dsn, args.user, getpass.getpass(prompt="Enter password: "))

    else:
        raise ValueError(f"Unsupported database type: {args.database}")

    execute_queries_concurrently(db, sql_query, args.instances, args.fetch_size)


if __name__ == "__main__":
    main()
