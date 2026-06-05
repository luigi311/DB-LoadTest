import time
from datetime import datetime

from databricks import sql
from src.functions import future_thread_executor

# Print a progress heartbeat every N fetched batches when fetch_size > 0.
progress_every: int = 5


class DatabricksDB:
    def __init__(
        self,
        server_hostname: str,
        http_path: str,
        access_token: str,
        printing: bool = False,
        prefix: str = "",
        fetch_size: int = 0,
    ):
        self.server_hostname: str = server_hostname
        self.http_path: str = http_path
        self.access_token: str = access_token
        self.printing: bool = printing
        self.prefix: str = prefix
        self.fetch_size: int = fetch_size

    def execute_query(self, sql_query: str, file_name: str):
        connection = None
        sql_query = f"{self.prefix}\n{sql_query}"

        try:
            connection = sql.connect(
                server_hostname=self.server_hostname,
                http_path=self.http_path,
                access_token=self.access_token,
            )

            if not connection:
                raise Exception(f"{file_name}: Could not connect to the database")
            else:
                print(f"{file_name}: Connected")

            with connection.cursor() as cursor:
                cursor.execute(sql_query)

                rows_fetched = 0
                if self.fetch_size > 0:
                    batches = 0
                    while True:
                        rows = cursor.fetchmany(self.fetch_size)

                        if not rows:
                            break

                        rows_fetched += len(rows)
                        batches += 1

                        # Heartbeat so a long-running fetch is visibly still
                        # making progress (vs genuinely stuck).
                        if batches % progress_every == 0:
                            now = datetime.now().strftime("%H:%M:%S")
                            print(
                                f"[{now}] {file_name}: still fetching... "
                                f"{rows_fetched} rows so far"
                            )

                        if self.printing:
                            for row in rows:
                                print(row)

                elif self.fetch_size == -1:
                    rows = cursor.fetchall()

                    rows_fetched = len(rows)

                    if self.printing:
                        for row in rows:
                            print(row)

                else:
                    # No-fetch mode. Databricks/Spark is lazy and streaming, and
                    # there is no scrollable-cursor "jump to last" or forward.
                    raise Exception("Databricks does not support fetch_size 0")

                if rows_fetched > 0:
                    print(f"{file_name}: Rows fetched: {rows_fetched}")

        finally:
            if connection:
                connection.close()

    def timer(self, sql_query: str, file_name: str):
        try:
            start_time = time.monotonic()
            self.execute_query(sql_query, file_name)
            end_time = time.monotonic()

            return file_name, end_time - start_time

        except Exception as e:
            print(f"{file_name}: Databricks-Error: {e}")
            return file_name, None

    def executor(self, bucket: list[tuple[str, str]]):
        # One thread per query in the bucket (override_threads) so they all fire
        # in parallel. Each arg is [callable, *positional_args] for the executor.
        args = [[self.timer, sql, file_name] for file_name, sql in bucket]

        thread_results = future_thread_executor(
            args, threads=len(bucket), override_threads=True
        )

        # Each result is (file_name, duration|None). Group successful runs by
        # file name; drop None (errored or thread-level failure).
        results: dict[str, list[float]] = {}
        for item in thread_results:
            if item is None:
                continue
            file_name, duration = item
            if duration is not None:
                results.setdefault(file_name, []).append(duration)

        return results

    def entry(self, bucket: list[tuple[str, str]]):
        return self.executor(bucket)
