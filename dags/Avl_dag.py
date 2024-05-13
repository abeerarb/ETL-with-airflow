from airflow.utils.dates import days_ago
from airflow import DAG
from datetime import timedelta
from avl_pipeline.groups.apis_group import get_units_api_call
from avl_pipeline.groups.mongo_group import mongo_group_tasks
from avl_pipeline.utilities import Utils


default_args = {
    'owner': 'abeer-araby',
    'depends_on_past': False,
    'email_on_failure': 'abeer.araby@afaqy.com',
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='AVL_DAG',
    default_args=default_args,
    description='A DAG to ETL units signals',
    schedule_interval= '@daily',  # Cron expression for every 20 minutes
    start_date=days_ago(1),
    catchup=False
) as dag:
    units_ids = get_units_api_call()
    mongo_grouping = mongo_group_tasks()
    concat_info = Utils.concat_groups_connection_string()
    

    units_ids >> mongo_grouping >> concat_info