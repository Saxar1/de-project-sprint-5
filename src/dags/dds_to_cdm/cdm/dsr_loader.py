from logging import Logger
from typing import List

from dds_to_cdm.cdm_settings_repository import EtlSetting, CdmEtlSettingsRepository
from lib import PgConnect
from datetime import date
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DsrObj(BaseModel):
    restaurant_id: str
    restaurant_name: str
    settlement_date: date
    orders_count: int
    orders_total_sum: float
    orders_bonus_payment_sum: float
    orders_bonus_granted_sum: float
    order_processing_fee: float
    restaurant_reward_sum: float


class DsrStgRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dsr(self, dsr_threshold: str) -> List[DsrObj]:
        with self._db.client().cursor(row_factory=class_row(DsrObj)) as cur:
            cur.execute(
                """
                    with 
                        metrics as (
                            select 
                                dr.id as restaurant_id,
                                dr.restaurant_name as restaurant_name,
                                dt."date" as settlement_date,
                                do2.id as order_id,
                                fps.total_sum as total_sum,
                                fps.bonus_payment as bonus_payment,
                                fps.bonus_grant as bonus_grant
                            from dds.dm_restaurants dr
                            join dds.dm_orders do2 on dr.id = do2.restaurant_id
                            join dds.dm_timestamps dt on do2.timestamp_id = dt.id 
                            join dds.fct_product_sales fps on do2.id = fps.order_id
                            where do2.order_status = 'CLOSED')
                    select
                        m.restaurant_id,
                        m.restaurant_name,
                        m.settlement_date,
                        count(distinct m.order_id) as orders_count,
                        sum(m.total_sum) as orders_total_sum,
                        sum(m.bonus_payment) as orders_bonus_payment_sum,
                        sum(m.bonus_grant) as orders_bonus_granted_sum,
                        sum(m.total_sum) * 0.25 as order_processing_fee,
                        (sum(m.total_sum) - sum(m.total_sum) * 0.25 - sum(m.bonus_payment)) as restaurant_reward_sum
                    from metrics as m
                    group by m.restaurant_id, m.restaurant_name, settlement_date
                    HAVING m.restaurant_id > %(threshold)s
                    ORDER BY m.restaurant_id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                """, {
                    "threshold": dsr_threshold
                }
            )
            objs = cur.fetchall()
        return objs


class DsrCdmRepository:
    def insert_dsr(self, conn: Connection, dsr: DsrObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                  """
                    INSERT INTO cdm.dm_settlement_report (restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(settlement_date)s, %(orders_count)s, %(orders_total_sum)s, %(orders_bonus_payment_sum)s, %(orders_bonus_granted_sum)s, %(order_processing_fee)s, %(restaurant_reward_sum)s)
                """,
                {
                    "restaurant_id": dsr.restaurant_id,
                    "restaurant_name": dsr.restaurant_name,
                    "settlement_date": dsr.settlement_date,
                    "orders_count": dsr.orders_count,
                    "orders_total_sum": dsr.orders_total_sum,
                    "orders_bonus_payment_sum": dsr.orders_bonus_payment_sum,
                    "orders_bonus_granted_sum": dsr.orders_bonus_granted_sum,
                    "order_processing_fee": dsr.order_processing_fee,
                    "restaurant_reward_sum": dsr.restaurant_reward_sum
                },
            )


class DsrLoader:
    WF_KEY = "Dsr_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DsrStgRepository(pg_dest)
        self.dds = DsrCdmRepository()
        self.settings_repository = CdmEtlSettingsRepository()
        self.log = log

    def load_dsr(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_dsr(last_loaded)
            self.log.info(f"Found {len(load_queue)} fps to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for dsr in load_queue:
                self.dds.insert_dsr(conn, dsr)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.restaurant_id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
