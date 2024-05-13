
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from avl_pipeline.globals import Globals
import logging 
import json
import requests as _ApiCaller
from bson import ObjectId


logger = logging.getLogger(__name__)

def login_parent(acc_name: str, acc_pass: str, **kwargs):
    ti = kwargs["ti"]
    data_payload = {
        "data": json.dumps({"username": acc_name, "password": acc_pass, "lang": "en", "expire": 24})}
    response = _ApiCaller.post(Globals.api_endpoint + "auth/login", data_payload)
    logger.debug(response.text)
    if response.status_code == 200:
            ti.xcom_push(key='parent_token', value=response.json()['data']['token'])
            logger.info(f"ti context info :{ti.xcom_pull(key='parent_token_1')}")
    else:
        raise Exception("")


def login_child(child_userid, **context):
    parent_token = context['ti'].xcom_pull(key='parent_token')
    data_payload = {"data": json.dumps({"id": child_userid})}
    response = _ApiCaller.post(Globals.api_endpoint + "auth/login_as?token=" + parent_token, data_payload)
    logger.debug(response.text)
    if response.status_code == 200:
        context['ti'].xcom_push(key='child_token', value= response.json()['data']['token'])
    else:
            raise Exception("Login failed with status code: {}".format(response.status_code))

def get_pagenation_count(**context):
    child_token=context['ti'].xcom_pull(key='child_token')
    request = _ApiCaller.post(Globals.api_endpoint+"units/lists?token=" + child_token)
    if request.status_code == 200:
        if request.json()["status_code"] == 200:
            context['ti'].xcom_push(key='units_count', value=request.json()["pagination"]['allCount'])
    else:
        raise Exception("Internal server error")
    
def get_all_units(**kwargs):
    child_token= kwargs['ti'].xcom_pull(key='child_token') 
    start=0
    limit = kwargs['ti'].xcom_pull(key='units_count')
    data_payload = {"data": json.dumps({"offset": start, "limit": limit,"projection": ["basic"]})}
    response = _ApiCaller.post(Globals.api_endpoint+"units/lists?token="+child_token, data_payload) 
    if response.status_code == 200:
        units_list = response.json()['data']
        ids_list = [ObjectId(unit['_id']) for unit in units_list]
        kwargs['ti'].xcom_push(key='units_ids', value= ids_list)
    else:
        logger.error("Error:", response.status_code)


def get_units_api_call():           

    with TaskGroup("get_units_from_APi", tooltip="Tasks for units_Api") as units_api:

            parent_login = PythonOperator(
            task_id='login_to_parent',
            python_callable=login_parent,
            op_kwargs={'acc_name': Globals.parent_username ,'acc_pass': Globals.parent_password}
            )

            child_login = PythonOperator(
            task_id='login_to_child',
            python_callable=login_child,
            op_kwargs={'child_userid':'5e57ed5a03ed52ad545fafb7'}# Globals.user_ids}
            )

            count_units = PythonOperator(
            task_id='get_pagenation_count',
            python_callable=get_pagenation_count
            )
                        
                        
            get_units = PythonOperator(
            task_id='get_all_units',
            python_callable=get_all_units
            )

            parent_login >> child_login >> count_units >> get_units
