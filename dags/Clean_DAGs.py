from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import DagRun, TaskInstance, XCom
from airflow.utils.db import provide_session
from airflow.models import XCom
from sqlalchemy import and_

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


from airflow.utils.db import provide_session
from airflow.models import XCom

@provide_session
def cleanup_xcom(session=None):
    session.query(XCom).filter(XCom.dag_id == "Units_statistics_DAG").delete()

with DAG(
    'clear_xcom_data_daily',
    default_args=default_args,
    description='Clear XCom data for specific DAGs daily',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False
) as dag:

    clear_xcom_data_task = PythonOperator(
        task_id='clear_xcom_data',
        python_callable=cleanup_xcom#,
        #op_args=['Units_statistics_DAG'],
    )

clear_xcom_data_task

