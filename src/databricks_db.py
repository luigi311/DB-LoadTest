import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from databricks import sql

# Print a progress heartbeat every N fetched batches when fetch_size > 0.
progress_every: int = 10


class DatabricksDB:
    def __init__(
        self,
        server_hostname: str,
        http_path: str,
        access_token: str,
        printing: bool = False,
        prefix: str = "",
    ):
        self.server_hostname: str = server_hostname
        self.http_path: str = http_path
        self.access_token: str = access_token
        self.printing: bool = printing
        self.prefix: str = prefix

    def execute_query(self, sql_query: str, file_name: str, fetch_size: int = 0):
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
                if fetch_size > 0:
                    batches = 0
                    while True:
                        rows = cursor.fetchmany(fetch_size)

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

                elif fetch_size == -1:
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

    def timer(self, sql_query: str, file_name: str, fetch_size: int = 0):
        try:
            start_time = time.monotonic()
            self.execute_query(sql_query, file_name, fetch_size)
            end_time = time.monotonic()

            return end_time - start_time

        except Exception as e:
            print(f"{file_name}: Databricks-Error: {e}")
            return None

    def executor(self, bucket: list[tuple[str, str]], fetch_size: int = 0):
        # One thread per query in the bucket so they all fire in parallel. The
        # threads spend almost all their time blocked on the network/server, so
        # the GIL is not the bottleneck here.
        results: dict[str, list[float]] = {}
        with ThreadPoolExecutor(max_workers=len(bucket)) as pool:
            future_to_file = {
                pool.submit(self.timer, sql, file_name, fetch_size): file_name
                for file_name, sql in bucket
            }

            for future in as_completed(future_to_file):
                file_name = future_to_file[future]
                duration = future.result()
                if duration is not None:
                    results.setdefault(file_name, []).append(duration)

        return results

    def entry(self, bucket: list[tuple[str, str]], fetch_size: int = 0):
        return self.executor(bucket, fetch_size)
