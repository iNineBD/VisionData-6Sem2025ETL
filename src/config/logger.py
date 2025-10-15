import json
import logging
import os
from datetime import datetime
from logging import StreamHandler

from config.elastic_client import ElasticClient


class ConsoleFormatter(logging.Formatter):
    """
    A formatter that ensures ALL console output is a JSON string.
    """

    def format(self, record):
        if isinstance(record.msg, dict):
            log_dict = record.msg
        else:
            log_dict = {
                "function": record.funcName,
                "action": "log_message",
                "level": record.levelname,
                "timestamp": datetime.fromtimestamp(record.created).isoformat(),
                "message": record.getMessage(),
            }
        return json.dumps(log_dict, ensure_ascii=False)


class ElasticDictFormatter(logging.Formatter):
    """
    Ensures that EACH log sent to Elasticsearch is a dictionary
    with a consistent structure.
    """

    def format(self, record):
        if isinstance(record.msg, dict):
            return record.msg

        iso_timestamp = datetime.fromtimestamp(record.created).isoformat()
        log_dict = {
            "function": record.funcName,
            "action": "log_message",
            "level": record.levelname,
            "timestamp": iso_timestamp,
            "message": record.getMessage(),
        }
        return log_dict


def setup_logger(logger_name):

    logger = logging.getLogger(logger_name)
    logger.setLevel(os.getenv("LOGGER_LEVEL", "INFO"))

    console_formatter = ConsoleFormatter()
    elastic_formatter = ElasticDictFormatter()

    if logger.hasHandlers():
        logger.handlers.clear()

    logger_output = os.getenv("LOGGER_OUTPUT", "CONSOLE").split(",")

    if "FILE" in logger_output:
        fh = logging.FileHandler(os.getenv("LOGGER_FILE", "app.log"))
        fh.setFormatter(console_formatter)
        logger.addHandler(fh)

    if "CONSOLE" in logger_output:
        ch = StreamHandler()
        ch.setFormatter(console_formatter)
        logger.addHandler(ch)

    if "ELASTIC" in logger_output:
        eh = ElasticClient()
        eh.setFormatter(elastic_formatter)
        logger.addHandler(eh)

    return logger
