from datetime import datetime
from logging import Logger

from origin_to_stg import EtlSetting, StgEtlSettingsRepository
from origin_to_stg.delivery_dag.reader import CourierReader, DeliveryReader
from origin_to_stg.delivery_dag.pg_saver import SaverCouriers, SaverDeliveries

from lib import PgConnect
from lib.dict_util import json2str


class CourierLoader:
    _LOG_THRESHOLD = 2

    WF_KEY = "courier_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, collection_loader: CourierReader, pg_dest: PgConnect, pg_saver: SaverCouriers, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
                

            last_loaded_id_str = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            self.log.info(f"starting to load from last checkpoint: {last_loaded_id_str}")

            load_queue = self.collection_loader.get_couriers()
            self.log.info(f"Found {len(load_queue)} documents to sync from couriers collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_object_courier(conn, str(d["_id"]), str(d["name"]))

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing couriers.")

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t["_id"] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)

class DeliveryLoader:
    _LOG_THRESHOLD = 2

    WF_KEY = "delivery_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, collection_loader: DeliveryReader, pg_dest: PgConnect, pg_saver: SaverDeliveries, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            load_queue = self.collection_loader.get_deliveries()
            self.log.info(f"Found {len(load_queue)} documents to sync from deliveries collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_object_delivery(conn, str(d["order_id"]), str(d["order_ts"]), str(d["delivery_id"]), str(d["courier_id"]), str(d["address"]), str(d["delivery_ts"]), str(d["rate"]), str(d["sum"]), str(d["tip_sum"]))

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing deliveries.")

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t["order_ts"] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)
