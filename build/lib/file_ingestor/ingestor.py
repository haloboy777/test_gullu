from .db_connection import *
import asyncio
import random
import time
import csv
import random
import aiofiles
from aiocsv import AsyncReader
class Ingestor(Exception):
    def __init__(self, csv_filepath):
        self.csv_filepath = csv_filepath
    async def ingest_file_helper(self):
        connection_object = DBconnection()
        self.db_connection = connection_object.get_connection()
        self.cursor = self.db_connection.cursor()
        try:
            check_table_exits = "SHOW TABLES LIKE '%s' "% ('%products%')
            self.cursor.execute(check_table_exits)
            result = cursor.fetchone()
            print(result)
            if(result == "products"):
                try:
                    async with aiofiles.open(self.csv_filepath, mode="r", encoding="utf-8", newline="") as afp:
                        async for row in AsyncReader(afp):
                            if(row[0] != "name" and row[1] != "sku" and row[2]!= "description"):
                                try:
                                    query = "insert into products(name, sku, description) values(%s,%s,%s) on duplicate key update name=values(name), description=values(description)"
                                    val = (row[0], row[1], row[2])
                                    self.cursor.execute(query, val)
                                except Exception as e:
                                    raise e
                        self.db_connection.commit()
                except Exception as e:
                    raise e
            else:
                raise Exception("table `products` does not exit!! stopping the process, create table products and start the process again")
        except Exception as e:
            raise e
        connection_object.close_connection(self.db_connection)
    def ingest_file(self):
        asyncio.run(ingest_file_helper())
    def aggregate_data(self):
        connection_object = DBconnection()
        self.db_connection = connection_object.get_connection()
        self.cursor = self.db_connection.cursor()
        try:
            check_table_exits = "SHOW TABLES LIKE '%s' "% ('%products_agg%')
            self.cursor.execute(check_table_exits)
            result = self.cursor.fetchone()[0]
            print("result:", result)
            if(result == "products_agg"):
                try:
                    agg_query = "select name,count(sku) from products group by name"
                    self.cursor.execute(agg_query)
                    agg_data = self.cursor.fetchall()
                    if(agg_data != []):
                        try:
                            query = "insert into products_agg(name, `no. of products`) values(%s,%s) on duplicate key update `no. of products` = values(`no. of products`)"
                            self.cursor.executemany(query, agg_data)
                            self.db_connection.commit()
                        except Exception as e:
                            raise e
                    else:
                        raise Exception("No data to be aggregated")
                except Exception as e:
                    raise e
            else:
                print(result)
                raise Exception("table `products_agg` does not exit!! stopping the process, create table `products_agg` and start the process again")
        except Exception as e:
            raise e
        connection_object.close_connection(self.db_connection)


        



