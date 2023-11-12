import json
from datetime import datetime
from typing import List, Optional
 
from lib import PgConnect
from logging import Logger
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
 
from stg_to_dds import DdsEtlSettingsRepository, EtlSetting
from stg_to_dds.dds.rest_loader import (RestaurantDdsRepository, RestaurantJsonObj,
                                   RestaurantRawRepository)
 
 
class ProductDdsObj(BaseModel):
    id: int
 
    product_id: str
    product_name: str
    product_price: float
 
    active_from: datetime
    active_to: datetime
 
    restaurant_id: int
 
 
class ProductDdsRepository:
    def insert_dds_products(self, conn: Connection, products: List[ProductDdsObj]) -> None:
        with conn.cursor() as cur:
            for product in products:
                cur.execute(
                    """
                        INSERT INTO dds.dm_products(
                            product_id,
                            product_name,
                            product_price,
                            active_from,
                            active_to,
                            restaurant_id)
                        VALUES (
                            %(product_id)s,
                            %(product_name)s,
                            %(product_price)s,
                            %(active_from)s,
                            %(active_to)s,
                            %(restaurant_id)s);
                    """,
                    {
                        "product_id": product.product_id,
                        "product_name": product.product_name,
                        "product_price": product.product_price,
                        "active_from": product.active_from,
                        "active_to": product.active_to,
                        "restaurant_id": product.restaurant_id
                    },
                )
 
    def get_product(self, conn: Connection, product_id: str) -> Optional[ProductDdsObj]:
        with conn.cursor(row_factory=class_row(ProductDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, product_id, product_name, product_price, active_from, active_to, restaurant_id
                    FROM dds.dm_products
                    WHERE product_id = %(product_id)s;
                """,
                {"product_id": product_id},
            )
            obj = cur.fetchone()
        return obj
 
    def list_products(self, conn: Connection) -> List[ProductDdsObj]:
        with conn.cursor(row_factory=class_row(ProductDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, product_id, product_name, product_price, active_from, active_to, restaurant_id
                    FROM dds.dm_products;
                """
            )
            obj = cur.fetchall()
        return obj
 
 
class ProductLoader:
    WF_KEY = "menu_products_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
 
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.dwh = pg
        self.raw = RestaurantRawRepository()
        self.dds_products = ProductDdsRepository()
        self.dds_restaurants = RestaurantDdsRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log
 
    def parse_restaurants_menu(self, restaurant_raw: RestaurantJsonObj, restaurant_version_id: int) -> List[ProductDdsObj]:
        res = []
        rest_json = json.loads(restaurant_raw.object_value)
        for prod_json in rest_json['menu']:
            t = ProductDdsObj(id=0,
                              product_id=prod_json['_id'],
                              product_name=prod_json['name'],
                              product_price=prod_json['price'],
                              active_from=datetime.strptime(rest_json['update_ts'], "%Y-%m-%d %H:%M:%S"),
                              active_to=datetime(year=2099, month=12, day=31),
                              restaurant_id=restaurant_version_id
                              )
 
            res.append(t)
        return res
 
    def load_products(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
 
            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
 
            load_queue = self.raw.load_raw_restaurants(conn, last_loaded_id)
            load_queue.sort(key=lambda x: x.id)
 
            products = self.dds_products.list_products(conn)
            prod_dict = {}
            for p in products:
                prod_dict[p.product_id] = p
 
            for restaurant in load_queue:
                restaurant_version = self.dds_restaurants.get_restaurant(conn, restaurant.object_id)
                if not restaurant_version:
                    return
 
                products_to_load = self.parse_restaurants_menu(restaurant, restaurant_version.id)
                products_to_load = [p for p in products_to_load if p.product_id not in prod_dict]
                self.dds_products.insert_dds_products(conn, products_to_load)
 
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = restaurant.id
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)