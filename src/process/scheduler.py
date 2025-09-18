import concurrent.futures
import os

import aspectlib
from schedule import every

from config.aop_logging import log_execution
from config.logger import setup_logger

from .dw_etl_processor import DwEtlProcessor

# Importe os dois processadores de ETL
from .etl_processor import EtlProcessor

logger = setup_logger(__name__)
schedule_times = os.getenv("SCHEDULE_TIME", "00:10").split(",")


@log_execution  # <-- CORREÇÃO: Adicionado para logar este job
def run_elastic_job():
    """
    Função dedicada para executar o ETL do Elasticsearch.
    Será executada em sua própria thread.
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


@log_execution  # <-- CORREÇÃO: Adicionado para logar este job
def run_dw_job():
    """
    Função dedicada para executar o ETL do Data Warehouse.
    Será executada em sua própria thread.
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


# Loga a função orquestradora também
def run_parallel_etl_jobs():
    """
    Função que será chamada pelo agendador para executar ambos os ETLs
    em paralelo usando um ThreadPoolExecutor.
    """
    logger.info("Iniciando execução paralela dos jobs de ETL.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_elastic = executor.submit(run_elastic_job)
        future_dw = executor.submit(run_dw_job)

        results = {"elastic": future_elastic.result(), "dw": future_dw.result()}

    logger.info(f"Execução paralela concluída com os seguintes status: {results}")


aspectlib.weave(EtlProcessor, log_execution)
aspectlib.weave(DwEtlProcessor, log_execution)

# Agendar para cada horário definido na variável de ambiente
for horario in schedule_times:
    # every().day.at(horario.strip()).do(run_parallel_etl_jobs)
    every(20).seconds.do(run_parallel_etl_jobs)
