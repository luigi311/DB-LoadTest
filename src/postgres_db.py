import asyncpg
import time

max_fetch_size = 100_000_000

class PostgresDB:
    def __init__(self, dsn, user, password):
        self.dsn = dsn
        self.user = user
        self.password = password

    async def execute_query(self, sql_query, instance_id, fetch_size=0):
        conn = None

        try:
            conn = await asyncpg.connect(dsn=self.dsn, user=self.user, password=self.password)
            
            if not conn:
                raise("Could not connect to the database")
            
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

    async def run_query(self, sql_query, instance_id, fetch_size=0):
        try:
            start_time = time.time()
            await self.execute_query(sql_query, instance_id, fetch_size)
            end_time = time.time()
            duration = end_time - start_time

            return duration
        except Exception as e:
            print(f"Instance {instance_id}: Error: {e}")

