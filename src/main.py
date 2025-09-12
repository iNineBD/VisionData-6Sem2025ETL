
from schedule import run_pending
from process.scheduler import start_etl
from config.logger import setup_logger
from config.elastic_client import ElasticClient

logger = setup_logger(__name__)


if __name__ == "__main__":
    # Inicializa o ElasticClient para garantir que o índice seja criado
    elastic_client = ElasticClient()

    start_etl()

    while True:
        run_pending()
