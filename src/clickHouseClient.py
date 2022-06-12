import asyncio
from queue import Queue
import time
from typing import Optional
from clickhouse_driver import Client
import threading

from src.config import CLICKHOUSE_DB_NAME, CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD




# clickhouse_client.execute(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB_NAME}")
# clickhouse_client.execute(f"CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB_NAME}.lux (`datetime` DateTime, `battery` Int32, `humidity` Float32, `illuminance_lux` Int32, `linkquality` Int32, `temperature` Float32, `illuminance` Int32) ENGINE=StripeLog()")


class ClickHouseStorage:
    async_loop = None
    event_loop_thread = None
    query_queue = Queue(maxsize=0)


class ClickHouseDriver:
    def __init__(self,
                 clickhouse_client: Client,
                 child_self,
                 min_inserts_count: Optional[int] = None,
                 max_inserts_count: int = 100,
                 timeout_sec: int = 10,
                 clickhouse_storage: ClickHouseStorage = ClickHouseStorage()
                 ):
        self.clickhouse_client = clickhouse_client
        self.global_vars = clickhouse_storage
        if self.global_vars.async_loop is None:
            self.global_vars.async_loop = asyncio.new_event_loop()
        if self.global_vars.event_loop_thread is None or self.global_vars.event_loop_thread.is_alive() is False:
            self.global_vars.event_loop_thread = threading.Thread(target=self.event_loop, args=(self.global_vars.async_loop,))
            self.global_vars.event_loop_thread.daemon = True
            self.global_vars.event_loop_thread.start()
        asyncio.ensure_future(self.writer_checker(max_inserts_count, min_inserts_count, timeout_sec, child_self), loop=self.global_vars.async_loop)

    def event_loop(self, loop: asyncio.AbstractEventLoop):
        asyncio.set_event_loop(loop)
        asyncio.ensure_future(self.clickhouse_query_executor(loop), loop=loop)
        loop.run_forever()

    async def clickhouse_query_executor(self, loop: asyncio.AbstractEventLoop):
        while True:
            if not self.global_vars.query_queue.empty():
                query_dict: dict = self.global_vars.query_queue.get()
                self.clickhouse_client.execute(query_dict["query"], query_dict["values"], types_check=True)
                l = len(query_dict["values"])
                print(f"write to db ({l})")
            await asyncio.sleep(1)
        loop.stop()

    async def writer_checker(self, max_inserts_count: int, min_inserts_count: int, timeout: int, child_self):
        timer = time.time()
        while True:
            if len(child_self.values_list) >= max_inserts_count:
                self.global_vars.query_queue.put({"query": child_self.query, "values": child_self.values_list})
                child_self.values_list = []  # TODO: add blocking values_list
                print("size trigger")
            if time.time()-timer >= timeout and len(child_self.values_list) >= min_inserts_count:
                self.global_vars.query_queue.put({"query": child_self.query, "values": child_self.values_list})
                child_self.values_list = []
                print("timeout trigger")
                timer = time.time()
            await asyncio.sleep(1)


class ClickHouseWriter(ClickHouseDriver):
    def __init__(self,
                 clickhouse_client: Client,
                 create_table_query: str = None,
                 table: str = None,
                 values_names: list = None,
                 min_inserts_count: int = 1,
                 max_inserts_count: int = 100,
                 timeout_sec: int = 10
                 ):
        self.clickhouse_client = clickhouse_client
        if not create_table_query and not (table and values_names):
            raise Exception("Must set create_table_query or table and values parameters")

        if create_table_query:
            table = create_table_query.split("(")[0].split()[-1]
            values_names = create_table_query.split("`")[1::2]
            clickhouse_client.execute(create_table_query)

        self.max_inserts_count = max_inserts_count
        self.query = f"INSERT INTO {table} ({', '.join(values_names)}) VALUES"
        self.values_names = values_names
        self.values_list = []
        super().__init__(clickhouse_client=clickhouse_client, max_inserts_count=max_inserts_count, min_inserts_count=min_inserts_count, timeout_sec=timeout_sec, child_self=self)

    def add_values(self, values: dict):
        if type(values) is dict and set([*values]) != set(self.values_names):
            raise Exception("Insert query values do not match")
        self.values_list.append(values)


# if __name__ == "__main__":
#     from datetime import datetime
#     test = ClickHouseWriter(table=f"{CLICKHOUSE_DB_NAME}.logs", values_names=["datetime", "tags", "fields"])
#     test2 = ClickHouseWriter(table=f"{CLICKHOUSE_DB_NAME}.logs", values_names=["datetime", "tags", "fields"])
#     print("t1 1000")
#     for i in range(1000):
#         test.add_values({"datetime": datetime.now(), "tags": f"tag_{i}", "fields": f"field_{i}"})
#     print("t2 1000")
#     for i in range(1010, 3000):
#         test2.add_values({"datetime": datetime.now(), "tags": f"tag_{i}", "fields": f"field_{i}"})
#     time.sleep(10)
#     print("t1 10")
#     for i in range(1000, 1010):
#         test.add_values({"datetime": datetime.now(), "tags": f"tag_{i}", "fields": f"field_{i}"})

#     while True:
#         pass
