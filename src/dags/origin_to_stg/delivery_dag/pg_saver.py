from datetime import datetime
from typing import Any

from psycopg import Connection


class SaverCouriers:
    def save_object_courier(self, conn: Connection, courier_id: str, courier_name: str):
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.couriers(courier_id, courier_name)
                    VALUES (%(courier_id)s, %(courier_name)s)
                    ON CONFLICT (courier_id) DO UPDATE
                    SET
                        courier_name = EXCLUDED.courier_name;
                """,
                {
                    "courier_id": courier_id,
                    "courier_name": courier_name,
                }
            )

class SaverDeliveries:
    def save_object_delivery(self, conn: Connection, order_id: str, order_ts: str, delivery_id: str, courier_id: str, address: str, delivery_ts: str, rate: str, sum: str, tip_sum: str):
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliveries(order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum)
                    VALUES (%(order_id)s, %(order_ts)s, %(delivery_id)s, %(courier_id)s, %(address)s, %(delivery_ts)s, %(rate)s, %(sum)s, %(tip_sum)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        order_id = EXCLUDED.order_id,
                        order_ts = EXCLUDED.order_ts,
                        courier_id = EXCLUDED.courier_id,
                        address = EXCLUDED.address,
                        delivery_ts = EXCLUDED.delivery_ts,
                        rate = EXCLUDED.rate,
                        sum = EXCLUDED.sum,
                        tip_sum = EXCLUDED.tip_sum;
                """,
                {
                    "order_id": order_id,
                    "delivery_id": delivery_id,
                    "order_ts": order_ts,
                    "courier_id": courier_id,
                    "address": address,
                    "delivery_ts": delivery_ts,
                    "rate": rate,
                    "sum": sum,
                    "tip_sum": tip_sum,
                }
            )