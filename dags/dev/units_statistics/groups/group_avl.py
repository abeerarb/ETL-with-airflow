from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from dev.units_statistics.globals import Globals
import logging 
import json
import requests as _ApiCaller



logger = logging.getLogger(__name__)

child_userid= "5d63e51e03ed5292a42c24e6" #"afaqyic"
username= "abeer.araby" #parent_username
password= "2vmdnnKo3" #parent_password


@task
def login_parent(acc_name: str, acc_pass: str, **kwargs):
    data_payload = {"data": json.dumps({"username": acc_name, "password": acc_pass, "lang": "en", "expire": 24})}
    logger.info(f"payload: {data_payload}")
    response = _ApiCaller.post(Globals.api_endpoint + "auth/login", data_payload)
    logger.debug(f"response: {response.text}")
    if response.status_code == 200:
        kwargs["ti"].xcom_push(key='parent_token_1', value=response.json()['data']['token'])
        logger.info(f"ti context info :{kwargs["ti"].xcom_pull(key='parent_token_1')}")
    else:
        raise Exception("")

@task
def login_child(child_userid, **context):
    parent_token = context['ti'].xcom_pull(key='parent_token_1')
    data_payload = {"data": json.dumps({"id": child_userid})}
    response = _ApiCaller.post(Globals.api_endpoint + "auth/login_as?token=" + parent_token, data_payload)
    logger.debug(response.text)
    if response.status_code == 200:
        context['ti'].xcom_push(key='child_token', value=response.json()['data']['token'])
        return response.json()['data']['token'] 
    else:
            raise Exception("Login failed with status code: {}".format(response.status_code))
@task
def get_pagenation_count(**context):
    child_token=context['ti'].xcom_pull(key='child_token')
    request = _ApiCaller.post(Globals.api_endpoint+"units/lists?token=" + child_token)
    if request.status_code == 200:
        if request.json()["status_code"] == 200:
            context['ti'].xcom_push(key='units_count', value=request.json()["pagination"]['allCount'])
    else:
        raise Exception("Internal server error")



def avl_api_group():
     with TaskGroup("avl_grouping", tooltip="Tasks for avl api") as avl_group:
        parent= login_parent(username,password) 
        child= login_child(child_userid) 
        pagenation= get_pagenation_count() 
            
        parent >> child >> pagenation 
        return  avl_group
