import asyncio
from datetime import datetime

import oracledb

from src.progress import measure_query, query_phase, write

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

    def test_connection(self) -> bool:
        async def test():
            connection = await oracledb.connect_async(
                user=self.user, password=self.password, dsn=self.dsn
            )
            try:
                return connection is not None
            finally:
                if connection is not None:
                    await connection.close()

        try:
            return asyncio.run(test())
        except Exception as e:
            write(f"Connection to database failed: {e}")
            return False

    async def execute_query(self, sql_query: str, file_name: str):
        connection = None
        sql_query = f"{self.prefix}\n{sql_query}"

        try:
            connection: oracledb.AsyncConnection = await oracledb.connect_async(
                user=self.user, password=self.password, dsn=self.dsn
            )

            if not connection:
                raise Exception(f"{file_name}: Could not connect to the database")

            with connection.cursor(scrollable=True) as cursor:
                if self.fetch_size != 0:
                    cursor.arraysize = (
                        self.fetch_size if self.fetch_size > 0 else max_fetch_size
                    )
                query_phase("executing")
                await cursor.execute(sql_query)
                query_phase("fetching")

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
                            write(
                                f"[{now}] {file_name}: still fetching... "
                                f"{rows_fetched} rows so far"
                            )

                        if self.printing:
                            for row in rows:
                                write(row)

                elif self.fetch_size == -1:
                    rows = await cursor.fetchall()

                    rows_fetched = len(rows)

                    if self.printing:
                        for row in rows:
                            write(row)

                else:
                    await cursor.scroll(mode="last")

                if rows_fetched > 0:
                    write(f"{file_name}: Rows fetched: {rows_fetched}")

        finally:
            if connection:
                query_phase("closing")
                await connection.close()

    async def timer(self, sql_query: str, file_name: str):
        try:
            with measure_query(file_name) as outcome:
                await self.execute_query(sql_query, file_name)

            return outcome.duration_seconds

        except oracledb.DatabaseError as e:
            write(f"{file_name}: Oracle-Error-Code: {e.args[0].code}")
            write(f"{file_name}: Oracle-Error-Message: {e.args[0].message}")

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

    def entry(self, bucket: list[tuple[str, str]]):
        return asyncio.run(self.executor(bucket))
