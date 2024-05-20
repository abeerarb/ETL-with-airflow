
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from dev.units_statistics.globals import Globals
import logging 
import requests as _ApiCaller
import pandas as pd
import os 
logger = logging.getLogger(__name__)


def login_CRM(**kwargs):
    data_payload = {
    "userName": "amr.ahmed@afaqy.com",
    "password": "Temp@150",
    "loginType": 1
}
    response = _ApiCaller.post(Globals.api_crm_endpoint+"Security/LoginIC", json = data_payload)
    if response.status_code == 200:
           kwargs["ti"].xcom_push(key='CRM_token', value=response.json()['data']['token'])
    else:
        raise Exception("faild to login to CRM")

def export_devices(**kwargs):
    crm_login_token =kwargs['ti'].xcom_pull(key='CRM_token')
    data_payload = {"isSearchFilter": False, "searchKey": "", "search": {}, "pageSize": 10, "pageNumber": 0} 
    headers = {"Authorization": "Bearer "+crm_login_token }    
    response = _ApiCaller.post(Globals.api_crm_endpoint+"Device/ExportDevice", json = data_payload ,headers=headers ) 
    if response.status_code == 200:
        logger.info(f"export succeded :{response.status_code}")
        logger.info(f"export succeded :{response.json()['data']}")
        kwargs['ti'].xcom_push(key='devices_request_file', value= response.json()['data'])
    else:
        raise Exception('error')


def devices_data(**kwargs):
    # URL from which to download the Excel file
    url = kwargs['ti'].xcom_pull(key='devices_request_file')  # Extract the URLs    
    # Send a GET request to download the file
    response = _ApiCaller.get(url)
    response.raise_for_status()  # Raise an HTTPError on a bad status
    # Save the content to an Excel file
    output_path = Globals.CRM_devices_path #'devices.xlsx'
    with open(output_path, 'wb') as output:
        output.write(response.content) 

    

# def get_all_simCards(**kwargs):
#     crm_login_token =kwargs['ti'].xcom_pull(key='CRM_token')
#     data_payload = {"isSearchFilter": False, "searchKey": "", "search": {}, "pageSize": 10, "pageNumber": 0} 
#     headers = {
#     "Authorization": "Bearer "+crm_login_token }    
#     response = _ApiCaller.post(Globals.api_crm_endpoint+"SIMCard/GetAllSimCards", json = data_payload, headers=headers ) 
#     if response.status_code == 200:
#         sim_cards = response.json()['data']
#         kwargs['ti'].xcom_push(key='sim_cards', value= sim_cards)
#     else:
#         logger.error("Error:", response.status_code)


def crm_devices_group():           

    with TaskGroup("crm_group", tooltip="Tasks for crm api") as crm_devices_group:

            crm_login = PythonOperator(
            task_id='login_api',
            python_callable=login_CRM
            )  
            expoertDevices= PythonOperator(
            task_id='export_api_devices',
            python_callable=export_devices
            )   
             
            getDevices= PythonOperator(
            task_id='loaddevices_fromExcel',
            python_callable=devices_data
            )             
                        
            # get_simCards = PythonOperator(
            # task_id='get_all_simCards_api',
            # python_callable=get_all_simCards
            # )
            
            crm_login >> expoertDevices >> getDevices
            return  crm_devices_group

