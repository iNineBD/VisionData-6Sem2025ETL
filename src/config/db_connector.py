import os
import pyodbc
from .logger import setup_logger

logger = setup_logger(__name__)


class DBConnector:
    def __init__(self, db_name=None):
        self.db_name = db_name or os.getenv("DB_NAME")
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
            logger.info(f"Conectado ao banco de dados {self.db_name}")

        except Exception as e:
            logger.error(f"Erro ao conectar no banco de dados: {str(e)}")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Conexão fechada.")

    # Método para execução de queries SQLs customizados
    def execute_query(self, query, params=None):
        try:
            self.cursor.execute(query, params)
            self.connection.commit()
        except Exception as e:
            logger.error(
                f"Erro ao executar query: {query}. Parâmetros: {params}. Erro: {str(e)}"
            )
            self.connection.rollback()

    # Método para buscar dados (SELECT)
    def fetch_all(self, query, params=None):
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            logger.error(
                f"Erro ao executar query: {query}. Parâmetros: {params}. Erro: {str(e)}"
            )

    # Métodos CRUD básicos
    def select(self, table, columns="*", condition=None):
        if condition:
            condition_str = " AND ".join([f"{col} = ?" for col in condition.keys()])
            query = f"SELECT {', '.join(columns)} FROM {table} WHERE {condition_str}"
            return self.fetch_all(query, list(condition.values()))
        else:
            query = f"SELECT {', '.join(columns)} FROM {table}"
            return self.fetch_all(query)
