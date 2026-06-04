import asyncio
import time

import oracledb


class OracleDB:
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
        connection = None
        sql_query = f"{self.prefix}\n{sql_query}"

        try:
            connection = await oracledb.connect_async(
                user=self.user, password=self.password, dsn=self.dsn
            )

            if not connection:
                raise Exception(
                    f"Instance {instance_id}: Could not connect to the database"
                )
            else:
                print(f"Instance {instance_id}: Connected")

            with connection.cursor(scrollable=True) as cursor:
                await cursor.execute(sql_query)

                rows_fetched = 0
                if fetch_size > 0:
                    while True:
                        rows = await cursor.fetchmany(fetch_size)

                        if not rows:
                            break

                        rows_fetched += len(rows)

                        if self.printing:
                            for row in rows:
                                print(row)

                elif fetch_size == -1:
                    rows = await cursor.fetchall()

                    rows_fetched = len(rows)

                    if self.printing:
                        for row in rows:
                            print(row)

                else:
                    await cursor.scroll(mode="last")

                if rows_fetched > 0:
                    print(f"Instance {instance_id}: Rows fetched: {rows_fetched}")

        finally:
            if connection:
                await connection.close()

    async def timer(self, sql_query: str, instance_id: int, fetch_size: int = 0):
        try:
            start_time = time.time()
            await self.execute_query(sql_query, instance_id, fetch_size)
            end_time = time.time()

            return end_time - start_time

        except oracledb.DatabaseError as e:
            print(f"Instance {instance_id}: Oracle-Error-Code: {e.args[0].code}")
            print(f"Instance {instance_id}: Oracle-Error-Message: {e.args[0].message}")

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
