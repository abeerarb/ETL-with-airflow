import datetime
import logging
from airflow.utils.task_group import TaskGroup
from elasticsearch import Elasticsearch, helpers
from dev.units_statistics.globals import Globals
from airflow import DAG
from airflow.decorators import task
import pandas as pd

logger = logging.getLogger(__name__)


# class elasticsearch config
class ElkManager:

    def client (ips, basic_auth, **kwargs):
        elk_client = Elasticsearch(ips, basic_auth=basic_auth,
                                        verify_certs=False,
                                        # sniff_on_start=True,  # Sniff at the start to get a list of cluster nodes
                                        request_timeout=200,  # Overall request timeout in seconds
                                        # max_retries=3,  # Maximum number of retries before giving up
                                        retry_on_timeout=True  # Retry when a request times out
                                        )
        return elk_client
   
    def pull_index_data(es, index):
        # Use the scan helper to retrieve all documents
        all_docs = helpers.scan(
            client=es,
            index=index,
            query={
                "_source": ["device_imei"],  # Specify the fields you want to include
                "query": {"match_all": {}}  # Match all documents
            }
        )
        # List to store the extracted data
        data = []
        # Extract _id and imei from each document
        for doc in all_docs:
            doc_id = doc['_id']
            device_imei = doc['_source'].get('device_imei', None)
            data.append({'doc_id': doc_id, 'device_imei': device_imei})
        # Convert the list of dictionaries to a pandas DataFrame
        df = pd.DataFrame(data)
        return df

    def update_index_status(es, df_index_data, index_name):
    #  logger.info(f"index_name to be updated: {index_name}")
        df2 = pd.read_excel(Globals.CRM_devices_path)
        logger.info("Excel file read successfully.")

        left_join = pd.merge(df_index_data, df2, left_on='device_imei', right_on='IMEI', how='left')
        logger.info("Left Join completed.")
        logger.info(f"Number of rows in left_join: {left_join.shape[0]}")
        logger.info(left_join.iloc[0])  # Print the first row


        actions = []
        for _, row in left_join.iterrows():
            # Convert GSM value to string, handling None values
            new_fields = {
                "GSM": row['GSM'] if not pd.isnull(row['GSM']) else None,
                "device_status": row['Status'] if not pd.isnull(row['Status']) else None
                # Add more new fields as needed
            }

            # Create the update action
            action = {
                '_op_type': 'update',
                '_index': index_name,#"units_statistics_airflow",
                '_id': row['doc_id'],
                'doc': new_fields,  # Specify the new fields to be updated
                'doc_as_upsert': True
            }
            actions.append(action)
        
        logger.info("Actions created successfully.")

        # Use helpers.bulk to update documents in bulk
        helpers.bulk(es, actions)
        logger.info("Bulk update completed.")
            
        # except Exception as e:
        #     logger.error(f"Error: {e}")




    def elk_pushData(es, index_name, docs_list, batch=10000):
        helpers.bulk(es, docs_list, index=index_name, chunk_size=batch)

    
    def delete_index(es, index_name):
        if es.indices.exists(index=index_name):
            result = es.indices.delete(index=index_name)
        else : 
            result = None
        return result


    def elk_closeConnection(es):
        es.transport.close()



