import json
from datetime import datetime, date, time
from typing import List, Optional

from lib import PgConnect
from logging import Logger
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from stg_to_dds import DdsEtlSettingsRepository, EtlSetting


class TSDdsObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    time: time
    date: date


class TSRawRepository:
    def load_raw_ts(self, conn: Connection, last_loaded_record_id: int) -> List[TSDdsObj]:
        with conn.cursor(row_factory=class_row(TSDdsObj)) as cur:
            cur.execute(
                """
                     SELECT
                        id,
                        (object_value::json ->> 'date')::timestamp as "ts",
                        date_part('year', (object_value::json ->> 'date')::timestamp) as "year",
                        date_part('month', (object_value::json ->> 'date')::timestamp) as "month",
                        date_part('day', (object_value::json ->> 'date')::timestamp) as "day",
                        (object_value::json ->> 'date')::timestamp::time as "time",
                        (object_value::json ->> 'date')::timestamp::date as "date"
                    FROM stg.ordersystem_orders 
                    WHERE id > %(last_loaded_record_id)s AND object_value::json ->> 'final_status' in ('CANCELLED', 'CLOSED');
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs


class TSDdsRepository:
    def insert_ts(self, conn: Connection, timestamp: TSDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s);
                """,
                {
                    "ts": timestamp.ts,
                    "year": timestamp.year,
                    "month": timestamp.month,
                    "day": timestamp.day,
                    "time": timestamp.time,
                    "date": timestamp.date
                },
            )

    def get_ts(self, conn: Connection, ts_id: str) -> Optional[TSDdsObj]:
        with conn.cursor(row_factory=class_row(TSDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        ts,
                        year,
                        month,
                        day,
                        time,
                        date
                    FROM dds.dm_timestamps
                    WHERE id = %(ts_id)s;
                """,
                {"ts_id": ts_id},
            )
            obj = cur.fetchone()
        return obj


class TSLoader:
    WF_KEY = "ts_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.dwh = pg
        self.raw = TSRawRepository()
        self.dds = TSDdsRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_ts(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            load_queue = self.raw.load_raw_ts(conn, last_loaded_id)
            load_queue.sort(key=lambda x: x.id)
            # ts_to_load = self.parse_ts(load_queue)
            for ts in load_queue:
                existing = self.dds.get_ts(conn, ts.id)
                if not existing:
                    self.dds.insert_ts(conn, ts)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = ts.id 
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)