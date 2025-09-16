import os

import aspectlib
from schedule import every

from config.aop_logging import log_execution

# 1. Importe a nova classe que criamos
from .etl_processor import EtlProcessor

schedule_times = os.getenv("SCHEDULE_TIME", "00:10").split(",")


def scheduler():
    """
    Função que será chamada pelo agendador nos horários definidos.
    """
    etl_job = EtlProcessor()
    etl_job.execute()


aspectlib.weave(scheduler, log_execution)


# Agendar para cada horário definido na variável de ambiente
for horario in schedule_times:
    # every(5).seconds.do(scheduler)
    every().day.at(horario.strip()).do(scheduler)
