from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging  
from dev.units_statistics.utilities import Utils
from dev.units_statistics.elk_manager import ElkManager
from dev.units_statistics.globals import Globals
from dev.units_statistics.groups.group_crm_devices_status import crm_devices_group
from dev.units_statistics.groups.group_avl import avl_api_group


logger = logging.getLogger(__name__)

@task
def delete_elk_index(**kwargs):
    # Retrieve units_length 
    length = kwargs['ti'].xcom_pull(key='units_count')     
    # Process and store data in Elasticsearch in chunks
    if length > 0: 
        Elk_client = ElkManager.client(Globals.elk_ips, Globals.elk_basic_auth)
        del_index = ElkManager.delete_index(Elk_client,Globals.index_name)   
        close_elk = ElkManager.elk_closeConnection(Elk_client)
        logger.info(f"elk index_name {Globals.index_name} deleted , length is : {length} ")
    else:
        logger.info(f"elk index_name {Globals.index_name} isn't deleted , length is : {length} ")

@task
def update_elk_devices_status():
        Elk_client = ElkManager.client(Globals.elk_ips, Globals.elk_basic_auth)
        logger.info(f"client connection done")
        df_elk_data = ElkManager.pull_index_data(Elk_client,Globals.index_name)
        logger.info(f"pull_index_data done ")
        ElkManager.update_index_status(Elk_client, df_elk_data, Globals.index_name)
        logger.info(f"update status done ")
        ElkManager.elk_closeConnection(Elk_client)
        logger.info(f"connection elk closed")

    
default_args = {
    'owner': 'abeer-araby',
    'depends_on_past': False,
    'email_on_failure': 'abeer.araby@afaqy.com',
    'email_on_retry': False,
    'retries': 3
    ,'retry_delay': timedelta(minutes=1)  
}

@task
def split_data_into_chunks(**context):
    # Retrieve units_length 
    length = context['ti'].xcom_pull(key='units_count')    
    # Chunk size
    Globals.chunk_size = 10000   
    # Process and store data in Elasticsearch in chunks
    if length > 0: 
        etl_arg=[]   
        for i in range(0, length, Globals.chunk_size):
            chunk = min(Globals.chunk_size, length - i)
            logger.info(f"i = {i}, chunk = {chunk}")
            etl_arg.append([i,chunk])
    context['ti'].xcom_push(key='etl_arg', value=etl_arg)
    
    return etl_arg 

@task
def ETL_job(args,**kwargs):
    child_token=kwargs['ti'].xcom_pull(key='child_token')

    logger.info(f"i: {args[0]}, chunk: {args[1]}")
    logger.info(f"child token is::: {child_token}")

    ##extract
    units_list = Utils.get_all_units_api(args[0], args[1],child_token)
    logger.info(units_list[0])

    ## transform
    units_list_update =  Utils.units_transform(units_list)

    ##load

    Elk_client = ElkManager.client(Globals.elk_ips, Globals.elk_basic_auth)
    ElkManager.elk_pushData(Elk_client,Globals.index_name,units_list_update)
    ElkManager.elk_closeConnection(Elk_client)



with DAG(
    'dev1_Units_statistics_DAG',
    default_args=default_args,
    description='A DAG to ETL units from api to elasticsearch',
    schedule_interval='*/20 * * * *',  # Cron expression for every 20 minutes
    #schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False
) as dag:   
    
    AVL= avl_api_group()
    etl_arg= split_data_into_chunks()
    units_list = ETL_job.expand(args= etl_arg)
    delete_index= delete_elk_index()
    CRM= crm_devices_group() 
    update_elk_device_status = update_elk_devices_status()
    
    [AVL >> etl_arg >> delete_index >> units_list , CRM] >> update_elk_device_status