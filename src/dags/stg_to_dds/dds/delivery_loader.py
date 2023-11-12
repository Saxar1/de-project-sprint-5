from logging import Logger
from typing import List
from datetime import datetime

from stg_to_dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DeliveryObj(BaseModel):
    id: int
    order_id: str
    delivery_id: str
    courier_id: str
    address: str
    delivery_ts: datetime
    rate: int
    sum: int
    tip_sum: int

class DeliveryStgRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliveries(self, delivery_threshold: int, limit: int) -> List[DeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(DeliveryObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        order_id::varchar,
                        delivery_id::varchar,
                        courier_id::varchar,
                        address::varchar,
                        delivery_ts::timestamp,
                        rate::numeric(14,2),
                        sum::numeric(14,2),
                        tip_sum::numeric(14,2)
                    FROM stg.deliveries
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": delivery_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DeliveryDdsRepository:
    def insert_delivery(self, conn: Connection, delivery: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(id, order_id, delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum)
                    VALUES (%(id)s, %(order_id)s, %(delivery_id)s, %(courier_id)s, %(address)s, %(delivery_ts)s, %(rate)s, %(sum)s, %(tip_sum)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        order_id = EXCLUDED.order_id,
                        courier_id = EXCLUDED.courier_id,
                        delivery_id = EXCLUDED.delivery_id,
                        address = EXCLUDED.address,
                        delivery_ts = EXCLUDED.delivery_ts,
                        rate = EXCLUDED.rate,
                        sum = EXCLUDED.sum,
                        tip_sum = EXCLUDED.tip_sum;
                """,
                {
                    "id": delivery.id,
                    "order_id": delivery.order_id,
                    "courier_id": delivery.courier_id,
                    "delivery_id": delivery.delivery_id,
                    "address": delivery.address,
                    "delivery_ts": delivery.delivery_ts,
                    "rate": delivery.rate,
                    "sum": delivery.sum,
                    "tip_sum": delivery.tip_sum
                },
            )


class DeliveryLoader:
    WF_KEY = "deliveries_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveryStgRepository(pg_dest)
        self.dds = DeliveryDdsRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for delivery in load_queue:
                self.dds.insert_delivery(conn, delivery)

            # Сохраняем прогресс.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
