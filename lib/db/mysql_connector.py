import cx_Oracle
import mysql

from lib.db.core.connector import BaseDBConnector

class MySQLConnector(BaseDBConnector):
    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                user=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database_name,
            )
            return self.connection
        except mysql.connector.Error as error:
            self.logger.error(f"Error connecting to MySQL: {error}")
            return None