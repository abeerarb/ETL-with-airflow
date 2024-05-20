# global vars
class Globals:
    # elk_ips = "http://localhost:9200"
    elk_ips = ['https://dbelk1.afaqy.local:9200', 'https://dbelk2.afaqy.local:9200', 'https://dbelk3.afaqy.local:9200']
    elk_basic_auth = ("elastic", "d=m+jbB1xh0k*-ik5GQB")
    api_endpoint = "https://api.afaqy.sa/"
    api_crm_endpoint ="https://crmbackend.afaqy.sa/"
    chunk_size = 1000
    index_name= "units_statistics_airflow"
    CRM_username = 'amr.ahmed@afaqy.com'
    CRM_password = 'Temp@150'
