import os
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
        db_client = os.getenv("CLIENT_DB_NAME")
        dw_db = os.getenv("DW_DB_NAME")

        self.db_client = DBConnector(db_name=db_client)
        self.db_client.connect()

        self.extract_service = ExtractDwService(db_connection=self.db_client)
        self.transform_service = TransformDwService()

        self.dw_db = DBConnector(db_name=dw_db)

        self.load_service = LoadDwService(db_connection=self.dw_db)

    def extract_data(self):
        logger.info("DW ETL: Extracting data from source")
        time.sleep(2)
        raw_data = self.extract_service.extract_complete_tickets_data()
        logger.info("DW ETL: Data extraction completed")
        self.db_client.close()
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
        self.dw_db.connect()
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
            self.dw_db.close()


aspectlib.weave(DwEtlProcessor, log_execution)
