from avl_pipeline import mongo_manager as MongoManager
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from avl_pipeline.globals import Globals
import logging 
import json
from bson import ObjectId

logger = logging.getLogger(__name__)


def mongo_group_tasks():
    with TaskGroup( "MongoDb", tooltip="group mongo tasks")as mongo_task_group:
        MongoClient= PythonOperator(
                task_id='connect_to_mongo_server',
                python_callable= MongoManager.client,
                op_kwargs={'uri': Globals.master_uri ,'db_name': Globals.db_name,'replicaSetName': Globals.replicaSetName,'collection_name':Globals.collection_units}
                )

        # units_collection= PythonOperator(
        #     task_id='units_collection',
        #     python_callable= MongoManager.select_collection,
        #     op_kwargs={'db_name': Globals.db_name,'collection_name': Globals.collection_units}
        #     )

        # units_grouping= PythonOperator(
        #     task_id='units_grouping',
        #     python_callable= MongoManager.get_grouped_units_from_ids
        #     )

        # signals_connection_collection= PythonOperator(
        #     task_id='signals_connection_collection',
        #     python_callable= MongoManager.select_collection,
        #     op_kwargs={'db_name': Globals.db_name,'collection_name': Globals.collection_signals_con}
        #     )
        
        # signals_connections_details= PythonOperator(
        #     task_id='signals_connections_details',
        #     python_callable= MongoManager.get_connections_ips
        #     )

        # disconnect_client= PythonOperator(
        #     task_id='disconnect_server',
        #     python_callable= MongoManager.closeConnection
        # )
        MongoClient 
        # >> units_collection >> units_grouping >> signals_connection_collection >> signals_connections_details >> disconnect_client
        return mongo_task_group