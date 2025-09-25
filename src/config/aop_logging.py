from datetime import datetime

import aspectlib

from config.dotenv_loader import load_default_env
from config.logger import setup_logger

load_default_env()
logger = setup_logger(__name__)


@aspectlib.Aspect(bind=True)
def log_execution(func, *args, **kwargs):
    function_name = func.__name__

    # Start log with log level
    logger.info(
        {
            "function": function_name,
            "action": "start_execution",
            "level": "INFO",
            "timestamp": datetime.now().isoformat(),
        }
    )
    try:
        result = yield aspectlib.Proceed(*args, **kwargs)

        # End log with log level
        logger.info(
            {
                "function": function_name,
                "action": "end_execution",
                "level": "INFO",
                "timestamp": datetime.now().isoformat(),
            }
        )
        return result
    except Exception as e:
        # Exception log with all necessary details
        logger.error(
            {
                "function": function_name,
                "action": "exception",
                "level": "ERROR",
                "timestamp": datetime.now().isoformat(),
                "exception_name": type(e).__name__,
                "exception_message": str(e),
                "message": f"Exception caught in function: {function_name}",
            }
        )
        raise
