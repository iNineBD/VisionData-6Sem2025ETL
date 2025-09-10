import logging
import os
import json
from logging import StreamHandler
from config.elastic_handler import ElasticHandler
from datetime import datetime

# FORMATADOR DO CONSOLE MODIFICADO PARA UNIFICAR A SAÍDA
class ConsoleFormatter(logging.Formatter):
    """
    Um formatador que garante que TODA a saída do console seja uma string JSON.
    """
    def format(self, record):
        # Se a mensagem já for um dicionário, apenas o converte para JSON.
        if isinstance(record.msg, dict):
            log_dict = record.msg
        # Se for uma string (log manual), cria o dicionário padrão.
        else:
            log_dict = {
                "function": record.funcName,
                "action": "log_message",
                "level": record.levelname,
                "timestamp": datetime.fromtimestamp(record.created).isoformat(),
                "message": record.getMessage()
            }
        # Converte o dicionário final para uma string JSON.
        return json.dumps(log_dict, ensure_ascii=False)


class ElasticDictFormatter(logging.Formatter):
    """
    Garante que CADA log enviado ao Elasticsearch seja um dicionário
    com uma estrutura consistente.
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
            "message": record.getMessage()
        }
        return log_dict

def setup_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(os.getenv('LOGGER_LEVEL', 'INFO'))

    # Formatador para arquivos (se necessário)
    file_formatter = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Nossos formatadores para console e Elastic
    console_formatter = ConsoleFormatter()
    elastic_formatter = ElasticDictFormatter()

    if logger.hasHandlers():
        logger.handlers.clear()

    logger_output = os.getenv('LOGGER_OUTPUT', 'CONSOLE').split(',')

    if 'FILE' in logger_output:
        fh = logging.FileHandler(os.getenv('LOGGER_FILE', 'app.log'))
        fh.setFormatter(console_formatter)
        logger.addHandler(fh)

    if 'CONSOLE' in logger_output:
        ch = StreamHandler()
        # USAMOS O NOVO FORMATADOR UNIFICADO AQUI
        ch.setFormatter(console_formatter)
        logger.addHandler(ch)

    if 'ELASTIC' in logger_output:
        eh = ElasticHandler()
        eh.setFormatter(elastic_formatter)
        logger.addHandler(eh)

    return logger