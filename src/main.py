# ruff: noqa: F401
from schedule import run_pending

from config.elastic_client import ElasticClient
from config.logger import setup_logger
from process.scheduler import run_sequential_etl_jobs

logger = setup_logger(__name__)


if __name__ == "__main__":

    # Main loop to run scheduled jobs
    while True:
        run_pending()
