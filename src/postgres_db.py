import asyncio
import time

import asyncpg

max_fetch_size: int = 100_000_000


class PostgresDB:
    def __init__(
        self,
        dsn: str,
        user: str,
        password: str,
        printing: bool = False,
        prefix: str = "",
    ):
        self.dsn: str = dsn
        self.user: str = user
        self.password: str = password
        self.printing: bool = printing
        self.prefix: str = prefix

    async def execute_query(
        self, sql_query: str, instance_id: int, fetch_size: int = 0
    ):
        conn = None
        sql_query = f"{self.prefix}\n{sql_query}"

        try:
            conn = await asyncpg.connect(
                dsn=self.dsn, user=self.user, password=self.password
            )

            if not conn:
                raise Exception(
                    f"Instance {instance_id}: Could not connect to the database"
                )
            else:
                print(f"Instance {instance_id}: Connected")

            async with conn.transaction():
                cur = await conn.cursor(sql_query)

                rows_fetched = 0
                while True:
                    if fetch_size > 0:
                        rows = await cur.fetch(fetch_size)

                    elif fetch_size == -1:
                        rows = await cur.fetch(max_fetch_size)

                    else:
                        rows = await cur.forward(max_fetch_size)

                    if isinstance(rows, list):
                        rows_fetched += len(rows)
                    else:
                        rows_fetched += rows

                    if not rows:
                        break

                    if self.printing:
                        if isinstance(rows, list):
                            for row in rows:
                                print(row)

                if rows_fetched > 0:
                    print(f"Instance {instance_id}: {rows_fetched} rows")

        finally:
            if conn:
                await conn.close()

    async def timer(self, sql_query: str, instance_id: int, fetch_size: int = 0):
        try:
            start_time = time.monotonic()
            await self.execute_query(sql_query, instance_id, fetch_size)
            end_time = time.monotonic()

            return end_time - start_time

        except (asyncpg.PostgresError, OSError) as e:
            print(f"Instance {instance_id}: Postgres-Error: {e}")

    async def executor(self, bucket: list[tuple[str, str]], fetch_size: int = 0):
        tasks = []
        for instance, (_, sql) in enumerate(bucket, start=1):
            tasks.append(self.timer(sql, instance, fetch_size))

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
