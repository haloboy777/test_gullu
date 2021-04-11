import pymysql
import sys
from conf import *
class DBconnection(Exception):
    def __init__(self):
        self.db_name = Config.DATABASE_CONFIG['db_name']
        self.host = Config.DATABASE_CONFIG['host']
        self.user = Config.DATABASE_CONFIG['user']
        self.password = Config.DATABASE_CONFIG['password']
    def get_connection(self):
        db = pymysql.connect(db=self.db_name,host=self.host, user=self.user, password = self.password)
        if(db):
            return db
        else:
            raise Exception("DATABASE CONNECTION FAILED. APPLICATION COULD NOT BE STARTED. PLEASE TRY AGAIN OR USE DIFFERENT CONFIGURATION")
    def close_connection(self,db_conn):
        try:
            db_conn.close()
        except:
            raise Exception("DATABSE CONNECTION COULD NOT BE CLOSED")