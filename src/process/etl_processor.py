import time
import aspectlib
from config.aop_logging import log_execution
from config.db_connector import DBConnector
from config.logger import setup_logger
from config.elastic_client import ElasticClient

logger = setup_logger(__name__)


class EtlProcessor:
    def __init__(self):
        self.db_connector = DBConnector()
        self.elastic_client = ElasticClient()
        self.transformed_data = None  # Para guardar os dados transformados

    def extract_data(self):
        logger.info("Extraindo dados específicos")
        time.sleep(1)
        # Exemplo: Dados extraídos
        return {"id": "user123", "name": "John Doe", "visits": 15}

    def transform_data(self, extracted_data):
        logger.info("Transformando dados")
        time.sleep(2)
        # Exemplo: Adiciona um novo campo
        extracted_data["status"] = "active"
        self.transformed_data = extracted_data

    def load_data(self):
        logger.info("Carregando dados no Elasticsearch")
        if self.transformed_data:
            # Usa o ID dos dados como ID do documento no Elasticsearch
            doc_id = self.transformed_data.get("id")
            if doc_id:
                # <-- 3. Chame o método de upsert
                self.elastic_client.upsert_document(
                    doc_id=doc_id, data=self.transformed_data
                )
            else:
                logger.error("Não foi possível carregar os dados: ID não encontrado.")
        time.sleep(1)

    def execute(self):
        extracted = self.extract_data()
        self.transform_data(extracted)
        self.load_data()


aspectlib.weave(EtlProcessor, log_execution)
