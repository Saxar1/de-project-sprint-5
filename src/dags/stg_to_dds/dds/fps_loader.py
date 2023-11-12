from logging import Logger
from typing import List

from stg_to_dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class FPSObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float


class FPSStgRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_fps(self, fps_threshold: int) -> List[FPSObj]:
        with self._db.client().cursor(row_factory=class_row(FPSObj)) as cur:
            cur.execute(
                """
                    with
                        product_m as (
                            select
                                be.id as id,
                                json_array_elements(be.event_value::json -> 'product_payments')::json ->> 'product_id' as product_id,
                                be.event_value::json ->> 'order_id' as order_id,
                                (json_array_elements(be.event_value::json -> 'product_payments')::json ->> 'quantity')::int as "count",
                                (json_array_elements(be.event_value::json -> 'product_payments')::json ->> 'price')::numeric as price,
                                (json_array_elements(be.event_value::json -> 'product_payments')::json ->> 'product_cost')::numeric as total_sum,
                                (json_array_elements(be.event_value::json -> 'product_payments')::json ->> 'bonus_payment')::numeric as bonus_payment,
                                (json_array_elements(be.event_value::json -> 'product_payments')::json ->> 'bonus_grant')::numeric as bonus_grant
                            from stg.bonussystem_events be
                            where event_type = 'bonus_transaction'),
                        fps as (
                            select distinct
                                pm.id,
                                dp.id as product_id,
                                do2.id as order_id,
                                "count",
                                price,
                                total_sum,
                                bonus_payment,
                                bonus_grant
                            from product_m as pm
                            join dds.dm_products dp on dp.product_id = pm.product_id
                            join dds.dm_orders do2 on do2.order_key = pm.order_id
                            WHERE pm.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                            ORDER BY pm.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                        )
                    select 
                        *
                    from fps;
                """, {
                    "threshold": fps_threshold
                }
            )
            objs = cur.fetchall()
        return objs


class FPSDdsRepository:
    def insert_fps(self, conn: Connection, fps: FPSObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                  """
                    INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                """,
                {
                    "product_id": fps.product_id,
                    "order_id": fps.order_id,
                    "count": fps.count,
                    "price": fps.price,
                    "total_sum": fps.total_sum,
                    "bonus_payment": fps.bonus_payment,
                    "bonus_grant": fps.bonus_grant
                },
            )


class FPSLoader:
    WF_KEY = "fps_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = FPSStgRepository(pg_dest)
        self.dds = FPSDdsRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_fps(self):
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
            load_queue = self.origin.list_fps(last_loaded)
            self.log.info(f"Found {len(load_queue)} fps to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for fps in load_queue:
                self.dds.insert_fps(conn, fps)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
