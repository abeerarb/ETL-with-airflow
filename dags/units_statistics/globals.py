# global vars
class Globals:
    user_name = "al-khalidi"  # al-khalidi
    user_ids = ["5e57ed5a03ed52ad545fafb7", "5edc9ce503ed5252d3361fa8", "6256d942674c9170c17c2e25",
                "62bb1bb23c00423a7b3af933", "615d44e4a46644246428383a", "5d06311c03ed52cc5372fce5",
                "5c64510803ed52ac566d1954", "5fb0dead03ed522ac33fd5f8", "5d627ae303ed52814b468526",
                "6279215fc3ba604ee4538dd4"]
    master_uri = "avlmdbr1.afaqy.sa:27017,avlmdbr2.afaqy.sa:27017,avlmdbr3.afaqy.sa:27017"
    replicaSetName = "AFAQY_AVL_MASTERDB"
    db_name = "afaqy_avl"
    collection_units = "units"
    collection_signals_con = "signal_connections"
    # elk_ips = "http://localhost:9200"
    elk_ips = ['https://dbelk1.afaqy.local:9200', 'https://dbelk2.afaqy.local:9200', 'https://dbelk3.afaqy.local:9200']
    elk_basic_auth = ("elastic", "d=m+jbB1xh0k*-ik5GQB")
    start_date = 1693526400000  # datetime(2023, 9, 1)
    end_date = 1700092799000  # datetime(2023, 11, 15)
    collection_exec = "etl_summary_exec_status"
    batch_size = 50000
    time_range = 432000000  # 5 Days
    short_time_range = 43200000  # 12 Hours
    api_endpoint = "https://api.afaqy.sa/"
    every_hours = 24
    data_retention_days = 90
    chunk_size = 1000
    index_name= "units_statistics_airflow"
