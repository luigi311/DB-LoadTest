import oracledb
import time

from src.functions import future_thread_executor


class OracleDB:
    def __init__(self, dsn, user, password):
        self.dsn = dsn
        self.user = user
        self.password = password

    def execute_query(self, sql_query, instance_id, fetch_size=0):
        try:
            connection = oracledb.connect(
                user=self.user, password=self.password, dsn=self.dsn
            )

            if not connection:
                raise Exception(
                    f"Instance {instance_id}: Could not connect to the database"
                )
            else:
                print(f"Instance {instance_id}: Connected")

            with connection.cursor(scrollable=True) as cursor:
                cursor.execute(sql_query)

                if fetch_size > 0:
                    rows_fetched = 0

                    while True:
                        rows = cursor.fetchmany(fetch_size)

                        if not rows:
                            break

                        rows_fetched += len(rows)
                        print(f"Instance {instance_id}: Fetched {rows_fetched} rows")

                elif fetch_size == -1:
                    rows = cursor.fetchall()

                    print(f"Instance {instance_id}: Fetched {len(rows)} rows")

                else:
                    cursor.scroll(mode="last")

        finally:
            connection.close()

    def timer(self, sql_query, instance_id, fetch_size=0):
        try:
            start_time = time.time()
            self.execute_query(sql_query, instance_id, fetch_size)
            end_time = time.time()

            return end_time - start_time

        except oracledb.DatabaseError as e:
            print(f"Instance {instance_id}: Oracle-Error-Code: {e.args[0].code}")
            print(f"Instance {instance_id}: Oracle-Error-Message: {e.args[0].message}")

    def entry(self, sql_query, num_instances, fetch_size=0):
        oracledb.init_oracle_client()

        durations = []
        tasks = []

        # Use asyncio one https://github.com/oracle/python-oracledb/issues/353 is resolved with 2.3.0
        # and https://github.com/oracle/python-oracledb/issues/367
        for instance_id in range(num_instances):
            tasks.append([self.timer, sql_query, instance_id, fetch_size])

        for duration in future_thread_executor(tasks, num_instances):
            if duration:
                durations.append(duration)

            else:
                print(duration)

        return durations
