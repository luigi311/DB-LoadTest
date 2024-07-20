import oracledb
import time

class OracleDB:
    def __init__(self, dsn, user, password):
        self.dsn = dsn
        self.user = user
        self.password = password

    async def execute_query(self, sql_query, instance_id, fetch_size=0):
        connection = await oracledb.connect_async(user=self.user, password=self.password, dsn=self.dsn)

        if not connection:
            raise Exception("Could not connect to the database")

        async with connection.cursor(scrollable=True) as cursor:
            await cursor.execute(sql_query)
            if fetch_size > 0:
                rows_fetched = 0
                while True:
                    rows = await cursor.fetchmany(fetch_size)
                    if not rows:
                        break
                    rows_fetched += len(rows)
                    print(f"Instance {instance_id}: Fetched {rows_fetched} rows")


    async def run_query(self, sql_query, instance_id, fetch_size=0):
        try:
            start_time = time.time()
            await self.execute_query(sql_query, instance_id, fetch_size)
            end_time = time.time()
            duration = end_time - start_time

            return duration
        except oracledb.DatabaseError as e:
            print(f"Instance {instance_id}: Oracle-Error-Code: {e.args[0].code}")
            print(f"Instance {instance_id}: Oracle-Error-Message: {e.args[0].message}")
