import asyncio
import asyncpg
import time

from typing import Union


max_fetch_size: int = 100_000_000


class PostgresDB:
    def __init__(self, dsn: str, user: str, password: str):
        self.dsn = dsn
        self.user = user
        self.password = password

    async def execute_query(
        self, sql_query: str, instance_id: int, fetch_size: int = 0
    ):
        conn = None

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

                if fetch_size > 0:
                    rows_fetched = 0

                    while True:
                        rows = await cur.fetch(fetch_size)

                        if not rows:
                            break

                        rows_fetched += len(rows)

                    print(f"Instance {instance_id}: Fetched {rows_fetched} rows")

                elif fetch_size == -1:
                    rows = await cur.fetch(max_fetch_size)
                    print(f"Instance {instance_id}: Fetched {len(rows)} rows")

                else:
                    while True:
                        rows = await cur.forward(max_fetch_size)

                        if not rows:
                            break

        finally:
            if conn:
                await conn.close()

    async def timer(self, sql_query: str, instance_id: int, fetch_size: int = 0):
        start_time = time.time()
        await self.execute_query(sql_query, instance_id, fetch_size)
        end_time = time.time()

        return end_time - start_time

    async def executor(
        self, sql_query: Union[str, list[str]], num_instances: int, fetch_size: int = 0
    ):
        tasks = []
        if isinstance(sql_query, str):
            tasks = [
                self.timer(sql_query, i + 1, fetch_size) for i in range(num_instances)
            ]
        elif isinstance(sql_query, list):
            instance = 0
            for _ in range(num_instances):
                for query in sql_query:
                    instance += 1
                    tasks.append(self.timer(query, instance, fetch_size))
        else:
            raise Exception("Invalid SQL query type")

        durations = await asyncio.gather(*tasks)

        return durations

    def entry(
        self, sql_query: Union[str, list[str]], num_instances: int, fetch_size: int = 0
    ):
        return asyncio.run(self.executor(sql_query, num_instances, fetch_size))
