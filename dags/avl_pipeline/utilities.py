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
        units_group_by_signal_conn_id =  kwargs['ti'].xcom_pull(key='units_group_by_signal_conn_id')
        signals_connections_dict = kwargs['ti'].xcom_pull(key='units_connections_details')
        """ merge user units groups & connection details """
        for group in units_group_by_signal_conn_id:
            find_obj = list(filter(lambda n: n["_id"] == ObjectId(group["_id"]), signals_connections_dict))
            if len(find_obj) > 0:
                group["connection_details"] = find_obj[0]
        kwargs['ti'].xcom_push(key= 'units_group', value =units_group_by_signal_conn_id)