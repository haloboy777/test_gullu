from mysql.connector import connect, Error
import sys
from conf import *
class DBconnection(Exception):
    def __init__(self):
        self.db_name = Config.DATABASE_CONFIG['db_name']
        self.host = Config.DATABASE_CONFIG['host']
        self.user = Config.DATABASE_CONFIG['user']
        self.password = Config.DATABASE_CONFIG['password']
    def get_connection_with_db(self):
        try:
            db = connect(host=self.host,user=self.user,password=self.password,db='inventory')
            return db
        except Error as e:
            raise e
    def get_connection(self):
        try:
            db = connect(host=self.host,user=self.user,password=self.password)
            return db
        except Error as e:
            raise e
        
    def check_table(self, cursor,table_name):
        check_table_exits = "SHOW TABLES LIKE '%s' "% ('%'+table_name+'%')
        cursor.execute(check_table_exits)
        tables = cursor.fetchall()
        flag = 0
        if(len(tables) == 0):
            print("Table does not exits!!!")
            return flag
        else:
            tables = list(tables[0])
            for table in tables:
                if(table == table_name):
                    flag = 1
                    break
        return flag
    def create_table(self,cursor,table_name):
        print("Creating table '"+table_name+"'")
        if(table_name =="products"):
            sql_create  = "CREATE TABLE `products` (`name` varchar(255) DEFAULT NULL,\
            `sku` varchar(20) NOT NULL,\
            `description` text,\
            `created_on` datetime DEFAULT CURRENT_TIMESTAMP,\
            `updated_on` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\
            PRIMARY KEY (`sku`)\
            )"
        elif(table_name == "products_agg"):
            sql_create = "CREATE TABLE `products_agg` (\
            `name` varchar(255) NOT NULL,\
            `no. of products` int DEFAULT NULL,\
            `created_on` datetime DEFAULT CURRENT_TIMESTAMP,\
            `updated_on` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\
            PRIMARY KEY (`name`)\
            )"
        try:
            cursor.execute(sql_create)
        except Exception as e:
            raise Exception("Table '"+table_name+"' could not be created beacuse of the error: ",str(e))

    def close_connection(self,db_conn):
        try:
            db_conn.close()
        except:
            raise Exception("DATABSE CONNECTION COULD NOT BE CLOSED")