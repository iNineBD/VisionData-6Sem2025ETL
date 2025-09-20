import os

import aspectlib
from schedule import every

from config.aop_logging import log_execution
from config.logger import setup_logger

from .dw_etl_processor import DwEtlProcessor
from .etl_processor import EtlProcessor

logger = setup_logger(__name__)
schedule_times = os.getenv("SCHEDULE_TIME", "00:10").split(",")


@log_execution
def run_elastic_job():
    """
    Função dedicada para executar o ETL do Elasticsearch.
    """
    logger.info("Iniciando o job de ETL para o Elasticsearch...")
    try:
        etl_job_elastic = EtlProcessor()
        etl_job_elastic.execute()
        logger.info("Job de ETL para o Elasticsearch concluído com sucesso.")
        return "ELASTIC_ETL_SUCCESS"
    except Exception as e:
        logger.error(f"Erro ao executar o ETL para o Elasticsearch: {e}", exc_info=True)
        return "ELASTIC_ETL_FAILED"


@log_execution
def run_dw_job():
    """
    Função dedicada para executar o ETL do Data Warehouse.
    """
    logger.info("Iniciando o job de ETL para o Data Warehouse...")
    try:
        etl_job_dw = DwEtlProcessor()
        etl_job_dw.execute()
        logger.info("Job de ETL para o Data Warehouse concluído com sucesso.")
        return "DW_ETL_SUCCESS"
    except Exception as e:
        logger.error(
            f"Erro ao executar o ETL para o Data Warehouse: {e}", exc_info=True
        )
        return "DW_ETL_FAILED"


def run_sequential_etl_jobs():
    """
    Executa os jobs de ETL em sequência: primeiro o DW e depois o Elasticsearch.
    """
    logger.info("Iniciando execução sequencial dos jobs de ETL.")

    dw_result = run_dw_job()
    elastic_result = run_elastic_job()

    results = {"dw": dw_result, "elastic": elastic_result}

    logger.info(f"Execução sequencial concluída com os seguintes status: {results}")


aspectlib.weave(EtlProcessor, log_execution)
aspectlib.weave(DwEtlProcessor, log_execution)

# Agendar para cada horário definido na variável de ambiente
for horario in schedule_times:
    every().day.at(horario.strip()).do(run_sequential_etl_jobs)
    # every(20).seconds.do(run_sequential_etl_jobs)
