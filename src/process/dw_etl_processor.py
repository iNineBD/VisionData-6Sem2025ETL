import time

import aspectlib

from config.aop_logging import log_execution
from config.db_connector import DBConnector
from config.logger import setup_logger
from services.extract_dw_service import ExtractDwService
from services.load_dw_service import LoadDwService
from services.transform_dw_service import TransformDwService

logger = setup_logger(__name__)


class DwEtlProcessor:
    def __init__(self):
        self.db_source_connector = DBConnector()
        self.db_source_connector.connect()

        self.extract_service = ExtractDwService(db_connection=self.db_source_connector)
        self.transform_service = TransformDwService()
        self.load_service = LoadDwService(db_connection=self.db_source_connector)

    def extract_data(self):
        logger.info("DW ETL: Extraindo dados da fonte")
        time.sleep(2)
        raw_data = self.extract_service.extract_complete_tickets_data()
        logger.info("DW ETL: Extração de dados concluída")
        return raw_data

    def transform_data(self, extracted_data):
        logger.info("DW ETL: Transformando dados para o modelo dimensional")
        time.sleep(2)
        transformed = self.transform_service.transform(extracted_data)
        logger.info("DW ETL: Transformação dos dados concluída")
        return transformed

    def load_data(self, transformed_data):
        """Carrega dados no Data Warehouse"""
        logger.info("DW ETL: Carregando dados no Data Warehouse")
        if not transformed_data:
            logger.error("DW ETL: Nenhum dado transformado para carregar")
            return

        self.load_service.load(transformed_data)
        logger.info("DW ETL: Carregamento no DW concluído.")

    def execute(self):
        try:
            extracted = self.extract_data()
            if extracted and extracted.get("tickets"):
                transformed = self.transform_data(extracted)
                self.load_data(transformed)
            else:
                logger.info("DW ETL: Nenhum dado extraído para processar.")
        finally:
            self.db_source_connector.close()


aspectlib.weave(DwEtlProcessor, log_execution)
