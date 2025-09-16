# ruff: noqa: F401
from schedule import run_pending

from config.elastic_client import ElasticClient
from config.logger import setup_logger
from process.scheduler import scheduler

logger = setup_logger(__name__)


if __name__ == "__main__":

    elastic_client = ElasticClient()

    while True:
        run_pending()
