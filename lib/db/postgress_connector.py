import psycopg2

from lib.db.core.connector import BaseDBConnector


class PostgreSQLConnector(BaseDBConnector):
    def connect(self):
        try:
            self.connection = psycopg2.connect(
                user=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database_name,
            )
            return self.connection
        except psycopg2.Error as error:
            self.logger.error(f"Error connecting to PostgreSQL: {error}")
            return None