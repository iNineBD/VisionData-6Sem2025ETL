from scheduler import every
from config.aop_logging import log_execution
import aspectlib
import os

# 1. Importe a nova classe que criamos
from .etl_processor import EtlProcessor


schedule_times = os.getenv("SCHEDULE_TIME", "00:10").split(",")


def start_etl():
    """
    Função inicial que é chamada pelo main.py.
    Agora ela executa nosso processo de ETL baseado em classe.
    """
    # 2. Crie uma instância da classe e execute o processo
    etl_job = EtlProcessor() # O log registrará a chamada de __init__
    etl_job.execute()        # O log registrará execute, extract, transform e load

def scheduler():
    """
    Função que será chamada pelo agendador nos horários definidos.
    """
    # 3. Podemos reutilizar a mesma lógica aqui
    etl_job = EtlProcessor()
    etl_job.execute()


# Aplica o aspecto automaticamente nas funções do agendador
aspectlib.weave(start_etl, log_execution)
aspectlib.weave(scheduler, log_execution)


# Agendar para cada horário definido na variável de ambiente
for horario in schedule_times:
    every().day.at(horario.strip()).do(scheduler)