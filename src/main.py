# ruff: noqa: F401
from schedule import run_pending
<<<<<<< HEAD
from process.scheduler import scheduler
from config.logger import setup_logger
from config.elastic_client import ElasticClient
=======
from process.scheduler import start_etl
from config.logger import setup_logger
>>>>>>> 5585ebd (API-17-chore: remove unused imports from main.py and requirements.txt)

logger = setup_logger(__name__)


if __name__ == "__main__":

    elastic_client = ElasticClient()

    while True:
        run_pending()
