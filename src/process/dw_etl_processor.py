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
        logger.info("DW ETL: Extracting data from source")
        time.sleep(2)
        raw_data = self.extract_service.extract_complete_tickets_data()
        logger.info("DW ETL: Data extraction completed")
        return raw_data

    def transform_data(self, extracted_data):
        logger.info("DW ETL: Transforming data to dimensional model")
        time.sleep(2)
        transformed = self.transform_service.transform(extracted_data)
        logger.info("DW ETL: Data transformation completed")
        return transformed

    def load_data(self, transformed_data):
        """Loads data into the Data Warehouse"""
        logger.info("DW ETL: Loading data into the Data Warehouse")
        if not transformed_data:
            logger.error("DW ETL: No transformed data to load")
            return

        self.load_service.load(transformed_data)
        logger.info("DW ETL: Load into DW completed.")

    def execute(self):
        try:
            extracted = self.extract_data()
            if extracted and extracted.get("tickets"):
                transformed = self.transform_data(extracted)
                self.load_data(transformed)
            else:
                logger.info("DW ETL: No data extracted to process.")
        finally:
            self.db_source_connector.close()


aspectlib.weave(DwEtlProcessor, log_execution)
