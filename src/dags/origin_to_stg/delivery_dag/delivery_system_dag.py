from datetime import datetime, timedelta
import logging
import pendulum
from airflow.decorators import dag, task
from origin_to_stg import StgEtlSettingsRepository
from origin_to_stg.delivery_dag.loader import CourierLoader, DeliveryLoader
from origin_to_stg.delivery_dag.pg_saver import SaverCouriers, SaverDeliveries
from origin_to_stg.delivery_dag.reader import CourierReader, DeliveryReader

from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'project', 'origin', 'stg'],
    is_paused_upon_creation=True
)
def project_sprint5_origin_to_stg_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    settings_repository = StgEtlSettingsRepository()

    # Объявляем таск, который загружает данные.
    @task(task_id="courier_load")
    def load_courier():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = SaverCouriers()

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = CourierReader()

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = CourierLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    @task(task_id="delivery_load")
    def load_delivery():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = SaverDeliveries()

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = DeliveryReader()

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = DeliveryLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()    

    # Инициализируем объявленные таски.
    courier_loader = load_courier()
    delivery_loader = load_delivery()

    
    # Далее задаем последовательность выполнения тасков.
    courier_loader  # type: ignore
    delivery_loader

stg_to_dds_dag = project_sprint5_origin_to_stg_dag()
