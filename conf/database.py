import os
class Config(object):
    DATABASE_CONFIG = {
    'host': os.environ['host'],
    'db_name': os.environ['db_name'],
    'user': os.environ['user'],
    'password': os.environ['password']   
    }
