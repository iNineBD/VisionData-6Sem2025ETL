import aspectlib
from datetime import datetime
from config.dotenv_loader import load_default_env
from config.logger import setup_logger

load_default_env()
logger = setup_logger(__name__)


@aspectlib.Aspect(bind=True)
def log_execution(func, *args, **kwargs):
    function_name = func.__name__

    # Log de início com o nível do log
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

        # Log de fim com o nível do log
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
        # Log de exceção com todos os detalhes necessários
        logger.error(
            {
                "function": function_name,
                "action": "exception",
                "level": "ERROR",
                "timestamp": datetime.now().isoformat(),
                "exception_name": type(e).__name__,
                "exception_message": str(e),
                "message": f"Exceção capturada na função: {function_name}",
            }
        )
        raise
