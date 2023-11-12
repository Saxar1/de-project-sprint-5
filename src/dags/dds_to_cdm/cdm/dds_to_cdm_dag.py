import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from dds_to_cdm import CdmEtlSettingsRepository
from dds_to_cdm.cdm.dsr_loader import DsrLoader
from dds_to_cdm.cdm.dcl_loader import DclLoader


from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['project', 'sprint5', 'dds', 'cdm'],
    is_paused_upon_creation=True
)
def project_sprint5_dds_to_cdm_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    settings_repository = CdmEtlSettingsRepository()

    # Объявляем таск, который загружает данные.
    @task(task_id="dsr_load")
    def load_dsr():
        # создаем экземпляр класса, в котором реализована логика.
        dsr_loader = DsrLoader(dwh_pg_connect, log)
        dsr_loader.load_dsr()  # Вызываем функцию, которая перельет данные.

    @task(task_id="dcl_load")
    def load_dcl():
        # создаем экземпляр класса, в котором реализована логика.
        dcl_loader = DclLoader(dwh_pg_connect, log)
        dcl_loader.load_dcl()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    # dsr_loader = load_dsr()
    dcl_loader = load_dcl()
    
    # Далее задаем последовательность выполнения тасков.
    # dsr_loader  
    dcl_loader

stg_to_dds_dag = project_sprint5_dds_to_cdm_dag()
