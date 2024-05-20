import calendar
import datetime
import json
import logging
from airflow.decorators import dag, task
from bson import ObjectId

from avl_pipeline.globals import Globals
import requests as _ApiCaller

logger = logging.getLogger(__name__)


class Utils:
    @staticmethod
    @task
    def concat_groups_connection_string(**kwargs):
        units_group_by_signal_conn_id = json.loads( kwargs['ti'].xcom_pull(key='units_group_by_signal_conn_id'))
        logger.info(f"units_group_by_signal_conn_id:{units_group_by_signal_conn_id}")
        logger.info(f"units_group_by_signal_conn_id:{units_group_by_signal_conn_id[0]}")


        signals_connections_dict =json.loads( kwargs['ti'].xcom_pull(key='units_connections_details'))
        """ merge user units groups & connection details """
        for group in units_group_by_signal_conn_id:
            # logger.info(f"all group:{group}")
            logger.info(f"group['_id']['$oid']:{group['_id']['$oid']}")
            # logger.info(f"signals_connections_dict:{signals_connections_dict}")
            find_obj = list(filter(lambda n: n["_id"] == group["_id"]['$oid']), signals_connections_dict)
            logger.info(f"connection_details{find_obj}")
            if len(find_obj) > 0:
                group["connection_details"] = find_obj[0]
        kwargs['ti'].xcom_push(key= 'units_group', value =units_group_by_signal_conn_id)