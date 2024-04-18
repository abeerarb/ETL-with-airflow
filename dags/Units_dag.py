from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from globals import Globals
from datetime import datetime, timedelta
import json
import logging  
from globals import Globals
import requests as _ApiCaller
from utilities import Utils
from elk_manager import ElkManager

logger = logging.getLogger(__name__)

def cleanup_xcom(context):
    # Access the task instance object
    ti = context['task_instance']
    
    # Delete XCom data for the task
    ti.xcom_clear()



default_args = {
    'owner': 'abeer-araby',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3
    ,'retry_delay': timedelta(minutes=1)  
    #,'xcom_cleanup': True  
}
child_userid= "5d63e51e03ed5292a42c24e6" #"afaqyic"
username= "abeer.araby" #parent_username
password= "2vmdnnKo3" #parent_password

@task
def login_parent(acc_name: str, acc_pass: str, **kwargs):
    ti = kwargs["ti"]
    data_payload = {
        "data": json.dumps({"username": acc_name, "password": acc_pass, "lang": "en", "expire": 24})}
    response = _ApiCaller.post(Globals.api_endpoint + "auth/login", data_payload)
    logger.debug(response.text)
    if response.status_code == 200:
            ti.xcom_push(key='parent_token_1', value=response.json()['data']['token'])
            logger.info(f"ti context info :{ti.xcom_pull(key='parent_token_1')}")

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

@task 
def split_data_into_chunks(**context):#**kwargs
    # Retrieve units_length 
    length = context['ti'].xcom_pull(key='units_count')

    logger.info (f'type of length is :{type(length)}')
    
    # Chunk size
    Globals.chunk_size = 10000
    
    # Process and store data in Elasticsearch in chunks
    if length > 0: 
        Elk_client = ElkManager(Globals.elk_ips, Globals.elk_basic_auth)
        Elk_client.delete_index(Globals.index_name)   
        Elk_client.elk_closeConnection()
        etl_arg=[]   
        for i in range(0, length, Globals.chunk_size):
            chunk = min(Globals.chunk_size, length - i)
            logger.info(f"i = {i}, chunk = {chunk}")
            etl_arg.append([i,chunk])
    context['ti'].xcom_push(key='etl_arg', value=etl_arg)
    
    return etl_arg 

@task
def ETL_job(child_token,args):
    logger.info(f"i: {args[0]}, chunk: {args[1]}")
    logger.info(f"child token is::: {child_token}")

    ##extract
    units_list = Utils.get_all_units_api(args[0], args[1],child_token)
    logger.info(units_list[0])

    ## transform
    units_list_update =  Utils.units_transform(units_list)

    ##load

    Elk_client = ElkManager(Globals.elk_ips, Globals.elk_basic_auth)
    Elk_client.elk_pushData(Globals.index_name,units_list_update)
    Elk_client.elk_closeConnection()



with DAG(
    'Units_statistics_DAG',
    default_args=default_args,
    description='A DAG to ETL units from api to elasticsearch',
    schedule_interval='@hourly',
    start_date=days_ago(1),
    catchup=False,
    on_success_callback=cleanup_xcom,
    on_failure_callback=cleanup_xcom
) as dag:   

    parent= login_parent(username,password) 
    child= login_child(child_userid) 
    pagenation= get_pagenation_count() 
    etl_arg= split_data_into_chunks()
    units_list = ETL_job.partial(child_token=child).expand(args= etl_arg)

    parent >> child >>pagenation >> etl_arg>>units_list
