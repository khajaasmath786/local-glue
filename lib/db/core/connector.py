import json
import cx_Oracle
import logging
class BaseDBConnector:
    def __init__(self, username, password,host, database_name,port="1521"):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database_name = database_name
        self.connection = None
        self.logger = logging.getLogger(__name__)

    def connect(self):
        pass


