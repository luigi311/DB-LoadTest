import oracledb
import time

from typing import Union

from src.functions import future_thread_executor


class OracleDB:
    def __init__(self, dsn: str, user: str, password: str, printing: bool = False):
        self.dsn: str = dsn
        self.user: str = user
        self.password: str = password
        self.printing: bool = printing

    def execute_query(self, sql_query: str, instance_id: int, fetch_size: int = 0):
        connection = None
        
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

                rows_fetched = 0
                if fetch_size > 0:
                    while True:
                        rows = cursor.fetchmany(fetch_size)

                        if not rows:
                            break

                        rows_fetched += len(rows)
                        
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
                    cursor.scroll(mode="last")

                if rows_fetched > 0:
                    print(f"Instance {instance_id}: Rows fetched: {rows_fetched}")

        finally:
            if connection:
                connection.close()

    def timer(self, sql_query: str, instance_id: int, fetch_size: int = 0):
        try:
            start_time = time.time()
            self.execute_query(sql_query, instance_id, fetch_size)
            end_time = time.time()

            return end_time - start_time

        except oracledb.DatabaseError as e:
            print(f"Instance {instance_id}: Oracle-Error-Code: {e.args[0].code}")
            print(f"Instance {instance_id}: Oracle-Error-Message: {e.args[0].message}")

    def entry(
        self, sql_query: Union[str, list[str]], num_instances: int, fetch_size: int = 0
    ):
        # Requires oracle thick client to use scroll. We can switch to the thin client once that feature
        # is implemented. Author said it is planned https://github.com/oracle/python-oracledb/issues/367
        oracledb.init_oracle_client()

        tasks = []

        # Use asyncio one https://github.com/oracle/python-oracledb/issues/353 is resolved with 2.3.0
        # and https://github.com/oracle/python-oracledb/issues/367
        if isinstance(sql_query, str):
            tasks = [
                [self.timer, sql_query, i, fetch_size] for i in range(num_instances)
            ]
        elif isinstance(sql_query, list):
            instance = 0
            for _ in range(num_instances):
                for query in sql_query:
                    instance += 1
                    tasks.append([self.timer, query, instance, fetch_size])
        else:
            raise Exception("Invalid SQL query type")

        durations = []
        for duration in future_thread_executor(
            tasks, len(tasks), override_threads=True
        ):
            if duration:
                durations.append(duration)

        return durations
