import logging
import pymongo
from pymongo import MongoClient
from bson import ObjectId
from bson import json_util
import json
from avl_pipeline.globals import Globals



logger = logging.getLogger(__name__)

def client(uri, db_name, replicaSetName, collection_name, **kwargs):
    client = None
    if replicaSetName is not None:
        client = MongoClient(f'mongodb://{uri}/{db_name}?replicaSet={replicaSetName}',
                                read_preference=pymongo.ReadPreference.SECONDARY,
                                maxStalenessSeconds=7200,  # Adjust the max staleness as needed
                                serverSelectionTimeoutMS=300000
                                # socket_timeOut "handle to reopen" or connection_timeout
                                )  # Adjust the timeout as needed)
    else:
        client = MongoClient(f'mongodb://{uri}/{db_name}',
                                read_preference=pymongo.ReadPreference.SECONDARY,
                                maxStalenessSeconds=7200,  # Adjust the max staleness as needed
                                serverSelectionTimeoutMS=300000)

    db = client[db_name]
    logger.info(f"Established mongo connection: client {client}")
    units_list= kwargs['ti'].xcom_pull(key='units_ids')
    ids_list = [ObjectId(unit['_id']) for unit in units_list]
    logger.info(f"ids_list {ids_list[0:10]}")
    collection_units= db[collection_name] #kwargs['ti'].xcom_pull(key='collection')
    # Create an aggregation pipeline to group by signal_connection_id
    query = {'_id': {'$in': ids_list}}
    pipeline = [
        {"$match": query},  # Filter documents by user_id
        {"$project": {"_id": 1, "imei": 1, "signal_connection_id": 1}},  # Project specific fields
        {
            "$group": {
                "_id": "$signal_connection_id",  # Group by signal_connection_id
                "docs": {
                    "$push": "$$ROOT"
                },  # Store all matching documents in an array
            }
        },
        {
            "$addFields": {
                "num_docs": {"$size": "$docs"}
            }  # Add a new field that contains the size of the docs array
        },
        {
            "$sort": {"num_docs": -1}  # Sort by the size of the docs array in descending order
        },
    ]
    # Execute the aggregation query
    units_group_by_signal_conn_id = list(collection_units.aggregate(pipeline))
    logger.info(f"units group: {units_group_by_signal_conn_id[0]}")
    units_group_by_signal_conn_id2 =json.dumps(units_group_by_signal_conn_id, default=json_util.default)
    logger.info(f"units group2: {units_group_by_signal_conn_id2}")
    distinct_connections = collection_units.distinct("signal_connection_id", query)
    logger.info(f"distinct_connections: {distinct_connections}")
   
    total_docs = sum(group['num_docs'] for group in units_group_by_signal_conn_id)

    logger.info(f"Found: {total_docs} units separated across: {len(distinct_connections)} different replicas \n")

    kwargs['ti'].xcom_push(key='units_group_by_signal_conn_id', value = units_group_by_signal_conn_id2)
    signals_connection_collection = db[Globals.collection_signals_con]
    units_connections_details = list(signals_connection_collection.find({"_id": {"$in": [
        ObjectId(connection) for connection in distinct_connections
    ]}}, projection={
        "_id": 1,
        "name": 1,
        "connection_string": 1,
        "database": 1,
        "replica_set_name": 1,
    }))
    kwargs['ti'].xcom_push(key='units_connections_details', value =json.dumps(units_connections_details, default=json_util.default))
 #   kwargs['ti'].xcom_push(key='distinct_connections', value = distinct_connections)
    client.close()





# def select_collection(db_name, collection_name, **kwargs):
#     client = kwargs['ti'].xcom_pull(key='mongo_client')
#     db = client[db_name]
#     collection = db[collection_name]
#     kwargs['ti'].xcom_push(key='collection', value= collection)


# def closeConnection(**kwargs):
#     client = kwargs['ti'].xcom_pull(key='mongo_client')
#     return client.close()


# def get_grouped_units_from_ids(**kwargs):
#     units_list= kwargs['ti'].xcom_pull(key='units_ids')
#     ids_list = [ObjectId(unit['_id']) for unit in units_list]
#     collection_units= kwargs['ti'].xcom_pull(key='collection')
#     # Create an aggregation pipeline to group by signal_connection_id
#     query = {'_id': {'$in': ids_list}}
#     pipeline = [
#         {"$match": query},  # Filter documents by user_id
#         {"$project": {"_id": 1, "imei": 1, "signal_connection_id": 1}},  # Project specific fields
#         {
#             "$group": {
#                 "_id": "$signal_connection_id",  # Group by signal_connection_id
#                 "docs": {
#                     "$push": "$$ROOT"
#                 },  # Store all matching documents in an array
#             }
#         },
#         {
#             "$addFields": {
#                 "num_docs": {"$size": "$docs"}
#             }  # Add a new field that contains the size of the docs array
#         },
#         {
#             "$sort": {"num_docs": -1}  # Sort by the size of the docs array in descending order
#         },
#     ]
#     # Execute the aggregation query
#     units_group_by_signal_conn_id = list(collection_units.aggregate(pipeline))
#     distinct_connections = collection_units.distinct("signal_connection_id", query)

#     total_docs = sum(group['num_docs'] for group in units_group_by_signal_conn_id)
#     logger.info(f"Found: {total_docs} units separated across: {len(distinct_connections)} different replicas \n")

#     kwargs['ti'].xcom_push(key='units_group_by_signal_conn_id', value = units_group_by_signal_conn_id)
#     kwargs['ti'].xcom_push(key='distinct_connections', value = distinct_connections)


# def get_connections_ips( distinct_connections, signals_connection_collection, **kwargs):

#     # get collection name & connections ids
#     # signals_connection_collection = kwargs['ti'].xcom_pull(key='collection')
#     # distinct_connections = kwargs['ti'].xcom_pull(key='distinct_connections')

#     """   Convert the list to a list of ObjectId instances
#             Define the projection to include only specific fields
#                 Execute the query on units_connections_details """

#     units_connections_details = list(signals_connection_collection.find({"_id": {"$in": [
#         ObjectId(connection) for connection in distinct_connections
#     ]}}, projection={
#         "_id": 1,
#         "name": 1,
#         "connection_string": 1,
#         "database": 1,
#         "replica_set_name": 1,
#     }))
#     kwargs['ti'].xcom_push(key='units_connections_details', value =json.dumps(units_connections_details, default=json_util.default) )
