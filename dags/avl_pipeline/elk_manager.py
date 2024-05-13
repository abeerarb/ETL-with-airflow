import datetime
import logging
from airflow.decorators import task
from elasticsearch import Elasticsearch, helpers

logger = logging.getLogger(__name__)


# class elasticsearch config
class ElkManager:
    @task
    def __init__(self, ips, basic_auth):
        self.elk_client = Elasticsearch(ips, basic_auth=basic_auth,
                                        verify_certs=False,
                                        # sniff_on_start=True,  # Sniff at the start to get a list of cluster nodes
                                        request_timeout=200,  # Overall request timeout in seconds
                                        # max_retries=3,  # Maximum number of retries before giving up
                                        retry_on_timeout=True  # Retry when a request times out
                                        )
        self.elk_client.cluster.health()

    @task
    def delete_elk_index_period(self, index_name, start_date, end_date):
        query = {
            "query": {
                "bool": {
                    "filter": {
                        "range": {
                            "dtt": {
                                "gte": start_date,
                                "lte": end_date,
                            }
                        }
                    }
                }
            }
        }
        try:
            if self.elk_client.indices.exists(index=index_name):
                response = self.elk_client.delete_by_query(index=index_name, body=query)
                logger.info(
                    f"Deleted from elasticsearch {response.get('deleted', 0)} documents within the specified date range.")
        except Exception as e:  # TODO: TO be checked
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)

    @task
    def delete_elk_before_time(self, index_name, exact_time: datetime):
        query = {
            "query": {
                "bool": {
                    "filter": {
                        "range": {
                            "dtt": {
                                "lt": int(exact_time.timestamp() * 1000)
                            }
                        }
                    }
                }
            }
        }
        if self.elk_client.indices.exists(index=index_name):
            response = self.elk_client.delete_by_query(index=index_name, body=query)
            logger.info(
                f"Deleted from elasticsearch {response.get('deleted', 0)} documents within the specified date range.")

    @task
    def elk_closeConnection(self):
        self.elk_client.close()

    @task
    def elk_pushData(self, index_name, docs_list, batch=10000):
        helpers.bulk(self.elk_client, docs_list, index=index_name, chunk_size=batch)



    @task
    def delete_index(self, index_name):
        if self.elk_client.indices.exists(index=index_name):
            result = self.elk_client.indices.delete(index=index_name)
        else : 
            result = None
    
        return result

