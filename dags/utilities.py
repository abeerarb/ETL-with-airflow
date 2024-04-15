import json
import logging
from globals import Globals
import requests as _ApiCaller
from airflow import DAG
from airflow.decorators import task

logger = logging.getLogger(__name__)


class Utils:        
    @staticmethod
    def get_all_units_api(start,limit,child_token):
        data_payload = {"data": json.dumps({"offset": start, "limit": limit,"projection": ["basic","last_update"]})}
        response = _ApiCaller.post(Globals.api_endpoint+"units/lists?token="+child_token, data_payload) 
        if response.status_code == 200:
            return response.json()['data']
        else:
            logger.error("Error:", response.status_code)

    @staticmethod
    def units_transform(units):
    
        units_update = []
        for unit in units:
                units_update.append({
                    "name": unit.get('name', ''),
                    "spd": unit['last_update'].get('spd', 0) if 'last_update' in unit else 0,
                    "acc":  unit['last_update'].get('acc', 0) if 'last_update' in unit else 0,
                    "last_update": unit['last_update'].get('dtt', 0) if 'last_update' in unit else 0,
                    "device_imei": unit.get('imei', ''),
                    "device_serial": unit.get('device_serial', ''),
                    "sim_number": unit.get('sim_number', ''),
                    "sim_serial": unit.get('sim_serial', ''),
                    "device": unit.get('device', ''),
                    "company": unit.get('company', ''),
                    "user_id_units" :unit.get('user_id',''),
                    "account": unit.get("owner",''),
                    "operation_code": unit.get('operation_code', ''),
                    "vehicle_battery": unit['last_update']['prms'].get('ePwrV', 0) if 'last_update' in unit and 'prms' in unit['last_update'] else 0
                })
        return units_update




    