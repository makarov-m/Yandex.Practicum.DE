from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository
import pandas as pd
import json

class CdmMessageProcessor:
    def __init__(self,
                 kafka_consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 logger: Logger) -> None:

        self._kafka_consumer = kafka_consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        # forced
        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")
        processed_messages = 0;
        timeout: float = 3.0
        while processed_messages < self._batch_size:
                
            dct_msg = self._kafka_consumer.consume(timeout=timeout)
            self._logger.info(f"{datetime.utcnow()}: INPUT MESSAGE: {dct_msg}")
            self._logger.info(f"{datetime.utcnow()}: TYPE MESSAGE: {type(dct_msg)}")
            if dct_msg :
                dct_msg = json.loads(dct_msg)
                if len(dct_msg) == 0:
                    continue
            else:
                continue
            
            df = pd.DataFrame()
            df = pd.json_normalize(dct_msg) 
            df = df[df['message_type'] == 'cdm_event']
            # self._logger.info(f"{datetime.utcnow()}: DF = {df}")
            # self._logger.info(f"{datetime.utcnow()}: DF.columns = {df.columns}")

            user_category_counters = df[['payload.user_id','payload.category_id','payload.category_name','payload.product_id']]\
                .groupby(['payload.user_id','payload.category_id','payload.category_name'],as_index=False)\
                .agg('count')\
                .rename(columns={'payload.user_id':'user_id', 'payload.category_id':'category_id', 'payload.category_name':'category_name','payload.product_id':'order_cnt'})

            user_product_counters = df[['payload.user_id','payload.product_id','payload.product_name','payload.order_id']]\
                .groupby(['payload.user_id','payload.product_id','payload.product_name'],as_index=False)\
                .agg('count')\
                .rename(columns={'payload.user_id':'user_id', 'payload.product_id':'product_id', 'payload.product_name':'product_name','payload.order_id':'order_cnt'})

            self._logger.info(f"{datetime.utcnow()}: OUTPUT user_category_counters: {user_category_counters}")
            self._logger.info(f"{datetime.utcnow()}: OUTPUT user_product_counters: {user_product_counters}")

            user_category_counters.apply(lambda x: self._cdm_repository.user_category_counters_upsert(
                x['user_id'],
                x['category_id'],
                x['category_name'],
                x['order_cnt']
                )
                , axis=1
            )

            user_product_counters.apply(lambda x: self._cdm_repository.user_product_counters_upsert(
                x['user_id'],
                x['product_id'],
                x['product_name'],
                x['order_cnt']
                )
                , axis=1
            ) 
            # user_product_counters.apply
            processed_messages += 1

        self._logger.info(f"{datetime.utcnow()}: FINISH")