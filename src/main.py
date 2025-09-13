# ruff: noqa: F401
from schedule import run_pending
from process.scheduler import scheduler
from config.logger import setup_logger
from config.elastic_client import ElasticClient

logger = setup_logger(__name__)


if __name__ == "__main__":

    elastic_client = ElasticClient()

    while True:
        run_pending()
