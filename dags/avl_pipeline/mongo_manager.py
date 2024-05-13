# import logging
# #import pymongo
# #from bson import ObjectId
# from pymongo import MongoClient
# from airflow.decorators import task
# logger = logging.getLogger(__name__)


# class MongoManager:
#     @task
#     def __init__(self, uri, db_name, replicaSetName, signals=False):
#         if replicaSetName is not None:
#             self.client = MongoClient(f'mongodb://{uri}/{db_name}?replicaSet={replicaSetName}',
#                                       read_preference=pymongo.ReadPreference.SECONDARY,
#                                       maxStalenessSeconds=7200,  # Adjust the max staleness as needed
#                                       serverSelectionTimeoutMS=300000
#                                       # socket_timeOut "handle to reopen" or connection_timeout
#                                       )  # Adjust the timeout as needed)
#         else:
#             self.client = MongoClient(f'mongodb://{uri}/{db_name}',
#                                       read_preference=pymongo.ReadPreference.SECONDARY,
#                                       maxStalenessSeconds=7200,  # Adjust the max staleness as needed
#                                       serverSelectionTimeoutMS=300000)
#         self.client.server_info()
#         logger.info(f"Established mongo connection: mongodb://{uri}")

#     @task
#     def select_collection(self, db_name, collection_name):
#         db = self.client[db_name]
#         collection = db[collection_name]
#         return collection

#     @task
#     def closeConnection(self):
#         return self.client.close()

#     @staticmethod
#     @task
#     def get_grouped_units(user_id, collection_units):
#         # Create an aggregation pipeline to group by signal_connection_id
#         query = {'user_id': user_id}
#         pipeline = [
#             {"$match": query},  # Filter documents by user_id
#             {"$project": {"_id": 1, "imei": 1, "signal_connection_id": 1}},  # Project specific fields
#             {
#                 "$group": {
#                     "_id": "$signal_connection_id",  # Group by signal_connection_id
#                     "docs": {
#                         "$push": "$$ROOT"
#                     },  # Store all matching documents in an array
#                 }
#             },
#             {
#                 "$addFields": {
#                     "num_docs": {"$size": "$docs"}
#                 }  # Add a new field that contains the size of the docs array
#             },
#             {
#                 "$sort": {"num_docs": -1}  # Sort by the size of the docs array in descending order
#             },
#         ]
#         # Execute the aggregation query
#         units_group_by_signal_conn_id = list(collection_units.aggregate(pipeline))
#         distinct_connections = collection_units.distinct("signal_connection_id", query)
#         total_docs = sum(group['num_docs'] for group in units_group_by_signal_conn_id)

#         logger.info(f"Found: {total_docs} units separated across: {len(distinct_connections)} different replicas \n")
#         return units_group_by_signal_conn_id, distinct_connections

#     @staticmethod
#     @task
#     def get_grouped_units_from_ids(ids_list, collection_units):
#         # Create an aggregation pipeline to group by signal_connection_id
#         query = {'_id': {'$in': ids_list}}
#         pipeline = [
#             {"$match": query},  # Filter documents by user_id
#             {"$project": {"_id": 1, "imei": 1, "signal_connection_id": 1}},  # Project specific fields
#             {
#                 "$group": {
#                     "_id": "$signal_connection_id",  # Group by signal_connection_id
#                     "docs": {
#                         "$push": "$$ROOT"
#                     },  # Store all matching documents in an array
#                 }
#             },
#             {
#                 "$addFields": {
#                     "num_docs": {"$size": "$docs"}
#                 }  # Add a new field that contains the size of the docs array
#             },
#             {
#                 "$sort": {"num_docs": -1}  # Sort by the size of the docs array in descending order
#             },
#         ]
#         # Execute the aggregation query
#         units_group_by_signal_conn_id = list(collection_units.aggregate(pipeline))
#         distinct_connections = collection_units.distinct("signal_connection_id", query)
#         total_docs = sum(group['num_docs'] for group in units_group_by_signal_conn_id)

#         logger.info(f"Found: {total_docs} units separated across: {len(distinct_connections)} different replicas \n")
#         return units_group_by_signal_conn_id, distinct_connections

#     # function to find connections details on master signal_connections
#     @staticmethod
#     @task
#     def get_connections_ips(distinct_connections, signal_connections):
#         """   Convert the list to a list of ObjectId instances
#                 Define the projection to include only specific fields
#                     Execute the query on units_connections_details """
#         units_connections_details = list(signal_connections.find({"_id": {"$in": [
#             ObjectId(connection) for connection in distinct_connections
#         ]}}, projection={
#             "_id": 1,
#             "name": 1,
#             "connection_string": 1,
#             "database": 1,
#             "replica_set_name": 1,
#         }))
#         return units_connections_details
