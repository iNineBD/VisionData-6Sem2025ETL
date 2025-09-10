import time
import aspectlib
from config.aop_logging import log_execution
from config.db_connector import DBConnector
from config.logger import setup_logger

logger = setup_logger(__name__)

class EtlProcessor:
    """
    Classe que encapsula o processo de ETL, utilizando o conector de banco de dados
    e simulando as etapas de extração, transformação e carga.
    """
    def __init__(self):
        """Inicializa o processador e o conector do banco."""
        self.db_connector = DBConnector()
        # Exemplo de como você usaria o conector

    def extract_data(self):
        """Simula a extração de dados da origem."""
        # Exemplo: self.db_connector.fetch_all("SELECT * FROM sua_tabela_origem")
        logger.info("Extraindo dados específicos")
        time.sleep(1) # Simula o trabalho

    def transform_data(self):
        """Simula a transformação dos dados."""
        time.sleep(2) # Simula o trabalho

    def load_data(self):
        """Simula o carregamento dos dados no destino."""
        # Exemplo: self.db_connector.execute_query("INSERT INTO sua_tabela_destino ...")
        time.sleep(1) # Simula o trabalho

    def execute(self):
        """
        Orquestra a execução completa do processo de ETL.
        Cada chamada de método aqui será logada.
        """
        self.extract_data()
        self.transform_data()
        self.load_data()

# APLICA O ASPECTO DE LOG NA CLASSE INTEIRA
# Todos os métodos (__init__, extract_data, transform_data, load_data, execute)
# serão monitorados automaticamente.
aspectlib.weave(EtlProcessor, log_execution)