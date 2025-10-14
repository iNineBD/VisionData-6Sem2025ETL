import os
import time

import aspectlib

from config.aop_logging import log_execution
from config.db_connector import DBConnector
from config.elastic_client import ElasticClient
from config.logger import setup_logger
from services.extract_elastic_service import ExtractElasticService
from services.transforme_elastic_service import TransformeElasticService

logger = setup_logger(__name__)


class ElasticEtlProcessor:
    def __init__(self):
        db_name = os.getenv("CLIENT_DB_NAME")

        self.db_connector = DBConnector(db_name=db_name)
        self.db_connector.connect()
        self.elastic_client = ElasticClient()
        self.transformed_data = None
        self.extract_service = ExtractElasticService(db_connection=self.db_connector)
        self.transforme_service = TransformeElasticService()

    def extract_data(self):
        logger.info("Extracting data")
        time.sleep(2)
        raw_data = self.extract_service.extract_complete_tickets_data()
        logger.info("Data extraction completed")
        return raw_data

    def transform_data(self, extracted_data):
        logger.info("Transforming data")
        time.sleep(2)

        self.transformed_data = self.transforme_service.transform_tickets_batch(
            extracted_data=extracted_data
        )
        logger.info("Data transformation completed")

    def load_data(self):
        """Loads data into Elasticsearch using the optimized bulk helper."""
        logger.info("Loading data into Elasticsearch using bulk operation...")

        if not self.transformed_data:
            logger.error("No transformed data to load")
            return

        success_count, errors = self.elastic_client.bulk_upsert(self.transformed_data)

        if errors:
            logger.error(f"Load completed with {len(errors)} errors.")
        else:
            logger.info(
                f"Load completed successfully: {success_count} documents processed."
            )

    def execute(self):
        extracted = self.extract_data()
        self.db_connector.close()
        self.transform_data(extracted)
        self.load_data()


aspectlib.weave(ElasticEtlProcessor, log_execution)
