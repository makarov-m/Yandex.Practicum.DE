import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def user_category_counters_upsert(self, 	
                                      user_id: str,
                                      category_id: str,	
                                      category_name: str,
                                      order_cnt: int) -> None:
        # upsert user_category_counters

        # dds.h_user
        upsert_statement = """
            INSERT INTO cdm.user_category_counters
                (user_id, category_id, category_name, order_cnt)
            VALUES
                (
                    '{upsert_user_id}',
                    '{upsert_category_id}',
                    '{upsert_category_name}',
                    '{upsert_order_cnt}'
                )
            ON CONFLICT (user_id,category_id) DO UPDATE
            SET
                category_name = EXCLUDED.category_name,
                order_cnt = user_category_counters.order_cnt + EXCLUDED.order_cnt
            ;
        """.format(
            upsert_user_id=user_id,
            upsert_category_id=category_id,
            upsert_category_name=category_name,
            upsert_order_cnt=order_cnt
        )
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(upsert_statement)
    def user_product_counters_upsert(self, 	
                                      user_id: str,
                                      product_id: str,	
                                      product_name: str,
                                      order_cnt: int) -> None:
        # upsert user_product_counters

        # dds.h_user
        upsert_statement = """
            INSERT INTO cdm.user_product_counters
                (user_id, product_id, product_name, order_cnt)
            VALUES
                (
                    '{upsert_user_id}',
                    '{upsert_product_id}',
                    '{upsert_product_name}',
                    '{upsert_order_cnt}'
                )
            ON CONFLICT (user_id,product_id) DO UPDATE
            SET
                product_name = EXCLUDED.product_name,
                order_cnt = user_product_counters.order_cnt + EXCLUDED.order_cnt
            ;
        """.format(
            upsert_user_id=user_id,
            upsert_product_id=product_id,
            upsert_product_name=product_name,
            upsert_order_cnt=order_cnt
        )
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(upsert_statement)