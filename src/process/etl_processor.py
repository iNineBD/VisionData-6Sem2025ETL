import time

import aspectlib

from config.aop_logging import log_execution
from config.db_connector import DBConnector
from config.elastic_client import ElasticClient
from config.logger import setup_logger
from services.extract_dw_service import ExtractElasticService
from services.transforme_elastic_service import TransformeElasticService

logger = setup_logger(__name__)


class EtlProcessor:
    def __init__(self):
        self.db_connector = DBConnector()
        self.db_connector.connect()
        self.elastic_client = ElasticClient()
        self.transformed_data = None
        self.extract_service = ExtractElasticService(db_connection=self.db_connector)
        self.transforme_service = TransformeElasticService()

    def extract_data(self):
        logger.info("Extraindo dados")
        time.sleep(2)
        raw_data = self.extract_service.extract_complete_tickets_data()
        logger.info("Extração de dados concluída")
        return raw_data

    def transform_data(self, extracted_data):
        logger.info("Transformando dados")
        time.sleep(2)

        self.transformed_data = self.transforme_service.transform_tickets_batch(
            extracted_data=extracted_data
        )
        logger.info("Transformação dos dados concluída")

    def load_data(self):
        """Carrega dados no Elasticsearch"""
        logger.info("Carregando dados no Elasticsearch")

        if not self.transformed_data:
            logger.error("Nenhum dado transformado para carregar")
            return

        # Carrega cada documento
        for document in self.transformed_data:
            doc_id = document.get("ticket_id")
            if doc_id:
                self.elastic_client.upsert_document(doc_id=doc_id, data=document)
            else:
                logger.error("Documento sem ticket_id")

        logger.info(f"Carregamento concluído: {len(self.transformed_data)} documentos")

    def execute(self):
        extracted = self.extract_data()
        self.transform_data(extracted)
        self.load_data()
        self.db_connector.close()


aspectlib.weave(EtlProcessor, log_execution)
