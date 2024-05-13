import calendar
import datetime
import json
import logging
from airflow.decorators import dag, task

#from bson import ObjectId

from avl_pipeline.globals import Globals
import requests as _ApiCaller

logger = logging.getLogger(__name__)


class Utils:

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



    @staticmethod
    def get_actual_units(user_id):
        def login_to_parent(acc_name: str, acc_pass: str) -> dict:
            data_payload = {
                "data": json.dumps({"username": acc_name, "password": acc_pass, "lang": "en", "expire": 24})}
            request = _ApiCaller.post(Globals.base_url  + "auth/login", data_payload)
            logger.debug(request.text)
            if request.status_code == 200:
                return request.json()
            else:
                raise Exception("")

        def login_to_child(session_token: str, child_userid: str) -> dict:
            data_payload = {"data": json.dumps({"id": child_userid})}
            request = _ApiCaller.post(Globals.base_url + "auth/login_as?token=" + session_token, data_payload)
            logger.debug(request.text)
            if request.status_code == 200:
                return request.json()
            else:
                raise Exception("")

        def get_all_units(session_token: str, start, length):
            data_payload = {"data": json.dumps({"offset": start, "limit": length, "projection": ["basic"]})}
            request = _ApiCaller.post("https://api.afaqy.sa/units/lists?token=" + session_token, data_payload)
            if request.status_code == 200:
                if request.json()["status_code"] == 200:
                    return request.json()['data'], request.json()["pagination"]
            else:
                raise Exception("Internal server error")

        login_parent = login_to_parent("amr.ahmed", "Admin123456")["data"]
        child_login = login_to_child(login_parent['token'], user_id)
        units_list = get_all_units(child_login["data"]['token'], 0, 9000)[0]

        ids_list = [unit['_id'] for unit in units_list]

        return [ObjectId(xid) for xid in ids_list]

    @staticmethod  # TODO: To be enhanced
    def concat_groups_connection_string(units_group_by_signal_conn_id, signals_connections_dict: list):
        """ merge user units groups & connection details """
        for group in units_group_by_signal_conn_id:
            find_obj = list(filter(lambda n: n["_id"] == ObjectId(group["_id"]), signals_connections_dict))
            if len(find_obj) > 0:
                group["connection_details"] = find_obj[0]
        return units_group_by_signal_conn_id

    @staticmethod
    def check_unit_pipeline_log(unit_id, collection):
        query = {"unit_id": unit_id}
        projection = {
            "dtt_start": 1,
            "dtt_end": 1
        }
        dates_result = collection.find_one(query, projection)
        if dates_result is None:
            return Globals.start_date, Globals.end_date, Globals.time_range
        else:
            start_date = dates_result["dtt_end"]
            current_datetime = datetime.datetime.now()
            end_date = int(current_datetime.timestamp() * 1000)
            return start_date, end_date, Globals.short_time_range

    @staticmethod
    def calculate_3_months_back(unix_timestamp):
        # Convert Unix timestamp to datetime
        start_date = datetime.datetime.utcfromtimestamp(unix_timestamp)

        # Calculate 3 months back
        month = start_date.month - 3 if start_date.month > 3 else start_date.month + 9
        year = start_date.year if start_date.month > 3 else start_date.year - 1
        last_day_of_month = calendar.monthrange(year, month)[1]
        day = min(start_date.day, last_day_of_month)
        three_months_back = datetime.datetime(year, month, day)
        return three_months_back
    
    @staticmethod
    def login_to_parent(acc_name: str, acc_pass: str) -> dict:
        data_payload = {
            "data": json.dumps({"username": acc_name, "password": acc_pass, "lang": "en", "expire": 24})}
        request = _ApiCaller.post(Globals.base_url + "auth/login", data_payload)
        logger.debug(request.text)
        if request.status_code == 200:
            return request.json()
        else:
            raise Exception("")
    
    @staticmethod
    def login_to_child(session_token: str, child_userid: str) -> dict:
        data_payload = {"data": json.dumps({"id": child_userid})}
        request = _ApiCaller.post(Globals.base_url + "auth/login_as?token=" + session_token, data_payload)
        logger.debug(request.text)
        if request.status_code == 200:
            return request.json()
        else:
            raise Exception("")
        
    @staticmethod
    def get_pagination_count(session_token: str):
        request = _ApiCaller.post(Globals.base_url+"units/lists?token=" + session_token)
        if request.status_code == 200:
            if request.json()["status_code"] == 200:
                return request.json()["pagination"]['allCount']
        else:
            raise Exception("Internal server error")
    @staticmethod
    def get_all_units_api(session_token: str, start, limit):
        data_payload = {"data": json.dumps({"offset": start, "limit": limit,"projection": ["basic","last_update"]})}#"stream":True
        response = _ApiCaller.post(Globals.base_url+"units/lists?token="+session_token, data_payload) 
        if response.status_code == 200:
            return response.json()['data']
        else:
            logger.error("Error:", response.status_code)

    
        
    @staticmethod
    def get_all_users_api(session_token: str, start, length):
        data_payload = {"data": json.dumps({"offset": start, "limit": length, "projection": ["basic"]})}
        request = _ApiCaller.post("https://api.afaqy.sa/user/lists?token="+ session_token, data_payload) #
        if request.status_code == 200:
            if request.json()["status_code"] == 200:
                return request.json()['data']
        else:
            raise Exception("Internal server error")

