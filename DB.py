# import eventlet
# eventlet.monkey_patch()  # noqa
import psycogreen.eventlet
psycogreen.eventlet.patch_psycopg()  # noqa
from psycopg2 import pool
import logging
from os import getcwd, path
import json

logging.basicConfig(filename='db.log', level=logging.DEBUG)

class PoolConection:
    def __init__(self,config_filename):
        raise NotImplementedError

    def get_connection(self):
        raise NotImplementedError

    def release_connection(self):
        raise NotImplementedError


class PosgresPoolConnection(PoolConection):

    def __init__(self, config_filename):
        with open(config_filename, "r") as file:
            db_args = json.loads(file.read())
            pool_size = db_args.pop("pool_size")
        self.db_args = db_args
        # self.pool = pool.ThreadedConnectionPool(
        #     minconn=pool_size,
        #     maxconn=pool_size,
        #     **db_args
        # )

    def get_connection(self):
        # return self.pool.getconn()
        return connect(**self.db_args)

    def release_connection(self, connection):
        try:
            # self.pool.putconn(connection)
            connection.close()
            return True
        except Exception as e:
            logging.debug(str(e))
            return False

    def  __del__(self):
        try:
            # self.pool.closeall()
            return True
        except Exception as e:
            logging.debug(str(e))
            return False
