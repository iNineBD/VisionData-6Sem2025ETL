import os

import pyodbc

from .logger import setup_logger

logger = setup_logger(__name__)


class DBConnector:
    def __init__(self):
        self.db_name = os.getenv("DB_NAME")
        self.host = os.getenv("DB_HOST")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.port = os.getenv("DB_PORT", "1433")

        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            connection_string = (
                "DRIVER={ODBC Driver 17 for SQL Server};"
                f"SERVER={self.host},{self.port};"
                f"DATABASE={self.db_name};"
                f"UID={self.user};"
                f"PWD={self.password};"
            )

            self.connection = pyodbc.connect(connection_string)
            self.connection.autocommit = False
            self.cursor = self.connection.cursor()
            logger.info(f"Connected to database {self.db_name}")

        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    logger.info("Connection closed.")

    def execute_query(self, query, params=None):
        try:
            if not self.cursor:
                logger.error("Could not execute query: cursor is not available.")
                return

            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            self.connection.commit()
        except Exception as e:
            logger.error(
                f"Error executing query: {query}. Parameters: {params}. Error: {str(e)}"
            )
            self.connection.rollback()

    def fetch_all(self, query, params=None):
        try:
            if not self.cursor:
                logger.error("Could not fetch data: cursor is not available.")
                return None

            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            logger.error(
                f"Error executing query: {query}. Parameters: {params}. Error: {str(e)}"
            )
            return None

    def select(self, table, columns="*", condition=None):
        if condition:
            condition_str = " AND ".join([f"{col} = ?" for col in condition.keys()])
            query = f"SELECT {', '.join(columns)} FROM {table} WHERE {condition_str}"
            return self.fetch_all(query, list(condition.values()))
        else:
            query = f"SELECT {', '.join(columns)} FROM {table}"
            return self.fetch_all(query)
