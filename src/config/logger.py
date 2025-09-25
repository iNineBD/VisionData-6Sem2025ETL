import json
import logging
import os
from datetime import datetime
from logging import StreamHandler
from logging.handlers import RotatingFileHandler


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


def setup_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(os.getenv("LOGGER_LEVEL", "INFO"))

    console_formatter = ConsoleFormatter()

    if logger.hasHandlers():
        logger.handlers.clear()

    logger_output = os.getenv("LOGGER_OUTPUT", "CONSOLE").split(",")

    if "FILE" in logger_output:
        log_filename_base = os.getenv("LOGGER_FILE", "app.log")
        file_name, file_extension = os.path.splitext(log_filename_base)
        log_filename = (
            f"{file_name}_{datetime.now().strftime('%Y-%m-%d')}{file_extension}"
        )

        fh = RotatingFileHandler(
            log_filename, maxBytes=10 * 1024 * 1024, backupCount=500, encoding="utf-8"
        )
        fh.setFormatter(console_formatter)
        logger.addHandler(fh)

    if "CONSOLE" in logger_output:
        ch = StreamHandler()
        ch.setFormatter(console_formatter)
        logger.addHandler(ch)

    return logger
