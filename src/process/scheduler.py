import os

import aspectlib
from schedule import every

from config.aop_logging import log_execution
from config.logger import setup_logger

from .dw_etl_processor import DwEtlProcessor
from .elastic_etl_processor import ElasticEtlProcessor

logger = setup_logger(__name__)
schedule_times = os.getenv("SCHEDULE_TIME", "00:10").split(",")


@log_execution
def run_elastic_job():
    """
    Function dedicated to running the Elasticsearch ETL.
    """
    logger.info("Starting the ETL job for Elasticsearch...")
    try:
        etl_job_elastic = ElasticEtlProcessor()
        etl_job_elastic.execute()
        logger.info("ETL job for Elasticsearch completed successfully.")
        return "ELASTIC_ETL_SUCCESS"
    except Exception as e:
        logger.error(f"Error running ETL for Elasticsearch: {e}", exc_info=True)
        return "ELASTIC_ETL_FAILED"


@log_execution
def run_dw_job():
    """
    Function dedicated to running the Data Warehouse ETL.
    """
    logger.info("Starting the ETL job for the Data Warehouse...")
    try:
        etl_job_dw = DwEtlProcessor()
        etl_job_dw.execute()
        logger.info("ETL job for the Data Warehouse completed successfully.")
        return "DW_ETL_SUCCESS"
    except Exception as e:
        logger.error(f"Error running ETL for the Data Warehouse: {e}", exc_info=True)
        return "DW_ETL_FAILED"


def run_sequential_etl_jobs():
    """
    Runs the ETL jobs sequentially: first DW, then Elasticsearch.
    """
    logger.info("Starting sequential execution of ETL jobs.")

    dw_result = run_dw_job()
    elastic_result = run_elastic_job()

    results = {"dw": dw_result, "elastic": elastic_result}

    logger.info(
        f"Sequential execution completed with the following statuses: {results}"
    )


aspectlib.weave(ElasticEtlProcessor, log_execution)
aspectlib.weave(DwEtlProcessor, log_execution)

# Schedule for each time defined in the environment variable
for horario in schedule_times:
    every().day.at(horario.strip()).do(run_sequential_etl_jobs)
    # every(20).seconds.do(run_sequential_etl_jobs)  # Example: schedule every 20 seconds
