import asyncio
from datetime import datetime

import asyncpg

from src.progress import measure_query, query_phase, write

max_fetch_size: int = 100_000_000

# Print a progress heartbeat every N fetched batches when fetch_size > 0.
progress_every: int = 5


class PostgresDB:
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
            connection = await asyncpg.connect(
                dsn=self.dsn, user=self.user, password=self.password
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
            connection = await asyncpg.connect(
                dsn=self.dsn, user=self.user, password=self.password
            )

            if not connection:
                raise Exception(f"{file_name}: Could not connect to the database")

            query_phase("executing")
            async with connection.transaction():
                cur = await connection.cursor(sql_query)

                query_phase("fetching")
                rows_fetched = 0
                batches = 0
                while True:
                    if self.fetch_size > 0:
                        rows = await cur.fetch(self.fetch_size)

                    elif self.fetch_size == -1:
                        rows = await cur.fetch(max_fetch_size)

                    else:
                        rows = await cur.forward(max_fetch_size)

                    if isinstance(rows, list):
                        rows_fetched += len(rows)
                    else:
                        rows_fetched += rows

                    if not rows:
                        break

                    batches += 1

                    # Heartbeat so a long-running fetch is visibly still making
                    # progress (vs genuinely stuck). Only meaningful for the
                    # batched fetch_size > 0 path.
                    if self.fetch_size > 0 and batches % progress_every == 0:
                        now = datetime.now().strftime("%H:%M:%S")
                        write(
                            f"[{now}] {file_name}: still fetching... "
                            f"{rows_fetched} rows so far"
                        )

                    if self.printing:
                        if isinstance(rows, list):
                            for row in rows:
                                write(row)

                if rows_fetched > 0:
                    write(f"{file_name}: {rows_fetched} rows")

        finally:
            if connection:
                query_phase("closing")
                await connection.close()

    async def timer(self, sql_query: str, file_name: str):
        try:
            with measure_query(file_name) as outcome:
                await self.execute_query(sql_query, file_name)

            return outcome.duration_seconds

        except (asyncpg.PostgresError, OSError) as e:
            write(f"{file_name}: Postgres-Error: {e}")

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
