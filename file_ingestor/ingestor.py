from .db_connection import DBconnection
import asyncio
import time
import csv
import random
import aiofiles
from aiocsv import AsyncReader
class Ingestor(Exception):
    def __init__(self, csv_filepath):
        self.csv_filepath = csv_filepath
    
    async def ingest_file_helper(self):
        print("Starting Ingestion")
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
                print("Ingestion Completed")
        except Exception as e:
            raise e 
    def init_database(self):
        connection_object = DBconnection()
        self.db_connection = connection_object.get_connection()
        self.cursor = self.db_connection.cursor()
        create_db_query = "CREATE DATABASE IF NOT EXISTS inventory"
        self.cursor.execute(create_db_query)
        connection_object.close_connection(self.db_connection)

    def ingest_file(self):
        try:
            connection_object = DBconnection()
            self.db_connection = connection_object.get_connection_with_db()
            self.cursor = self.db_connection.cursor()
            if(connection_object.check_table(self.cursor, "products") == 0):
                connection_object.create_table(self.cursor, "products")
                asyncio.run(self.ingest_file_helper())
                connection_object.close_connection(self.db_connection)
            else:
                asyncio.run(self.ingest_file_helper())
                connection_object.close_connection(self.db_connection)
        except Exception as e:
            raise e

    def aggregate_data_helper(self):
        print("Starting Aggregating data")
        try:
            agg_query = "select name,count(sku) from products group by name"
            self.cursor.execute(agg_query)
            agg_data = self.cursor.fetchall()
            if(len(agg_data) != 0):
                try:
                    query = "insert into products_agg(name, `no. of products`) values(%s,%s) on duplicate key update `no. of products` = values(`no. of products`)"
                    self.cursor.executemany(query, agg_data)
                    self.db_connection.commit()
                    print("Aggregation Completed")
                except Exception as e:
                    raise e
            else:
                raise Exception("No data to be aggregated in the table `products`")
        except Exception as e:
            raise e       

    def aggregate_data(self):
        try:
            connection_object = DBconnection()
            self.db_connection = connection_object.get_connection_with_db()
            self.cursor = self.db_connection.cursor()
            if(connection_object.check_table(self.cursor, "products_agg") == 0):
                connection_object.create_table(self.cursor, "products_agg")
                self.aggregate_data_helper()
                connection_object.close_connection(self.db_connection)
            else:
                self.aggregate_data_helper()
                connection_object.close_connection(self.db_connection)
        except Exception as e:
            raise e


        



