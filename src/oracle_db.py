import asyncio
import time
from datetime import datetime

import oracledb

# Print a progress heartbeat every N fetched batches when fetch_size > 0.
progress_every: int = 5
max_fetch_size: int = 100_000_000


class OracleDB:
    def __init__(
        self,
        dsn: str,
        user: str,
        password: str,
        printing: bool = False,
        prefix: str = "",
        fetch_size: int = 0,
    ):
        self.dsn: str = dsn
        self.user: str = user
        self.password: str = password
        self.printing: bool = printing
        self.prefix: str = prefix
        self.fetch_size: int = fetch_size

    async def execute_query(self, sql_query: str, file_name: str):
        connection = None
        sql_query = f"{self.prefix}\n{sql_query}"

        try:
            connection = await oracledb.connect_async(
                user=self.user, password=self.password, dsn=self.dsn
            )

            if not connection:
                raise Exception(f"{file_name}: Could not connect to the database")
            else:
                print(f"{file_name}: Connected")

            with connection.cursor(scrollable=True) as cursor:
                if self.fetch_size != 0:
                    cursor.arraysize = (
                        self.fetch_size if self.fetch_size > 0 else max_fetch_size
                    )
                await cursor.execute(sql_query)

                rows_fetched = 0
                if self.fetch_size > 0:
                    batches = 0
                    while True:
                        rows = await cursor.fetchmany(self.fetch_size)

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
                    rows = await cursor.fetchall()

                    rows_fetched = len(rows)

                    if self.printing:
                        for row in rows:
                            print(row)

                else:
                    await cursor.scroll(mode="last")

                if rows_fetched > 0:
                    print(f"{file_name}: Rows fetched: {rows_fetched}")

        finally:
            if connection:
                await connection.close()

    async def timer(self, sql_query: str, file_name: str):
        try:
            start_time = time.time()
            await self.execute_query(sql_query, file_name)
            end_time = time.time()

            return end_time - start_time

        except oracledb.DatabaseError as e:
            print(f"{file_name}: Oracle-Error-Code: {e.args[0].code}")
            print(f"{file_name}: Oracle-Error-Message: {e.args[0].message}")

    async def executor(self, bucket: list[tuple[str, str]]):
        tasks = []
        for file_name, sql in bucket:
            tasks.append(self.timer(sql, file_name))

        durations = await asyncio.gather(*tasks)

        # Group durations by file name. A query that errored returns None from
        # timer; skip those so per-file stats only reflect successful runs.
        results: dict[str, list[float]] = {}
        for (file_name, _), duration in zip(bucket, durations):
            if duration is not None:
                results.setdefault(file_name, []).append(duration)

        return results

    def entry(self, bucket: list[tuple[str, str]], fetch_size: int = 0):
        return asyncio.run(self.executor(bucket, fetch_size))
