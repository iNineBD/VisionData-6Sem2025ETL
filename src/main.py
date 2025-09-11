from schedule import run_pending
from process.scheduler import start_etl
import time
from config.logger import setup_logger
import os

logger = setup_logger(__name__)

if __name__ == "__main__":

    start_etl()

    while True:
        run_pending()