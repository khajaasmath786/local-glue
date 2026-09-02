import cx_Oracle
from lib.db.core.connector import BaseDBConnector

class OracleDBConnector(BaseDBConnector):
    def connect(self):
        try:
            oracle_url = f"{self.username}/{self.password}@{self.host}:{self.port}/{self.database_name}"
            self.connection = cx_Oracle.connect(oracle_url)
            return self.connection
        except cx_Oracle.Error as error:
            self.logger.error(f"Error connecting to Oracle: {error}")
            return None