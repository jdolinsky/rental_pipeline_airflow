from airflow import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# GCP stuff
LOCATION = "us-central1"
BUCKET = "rental_transactions"
STORAGE_DOC = "new_transactions.csv"

#executables
def get_max_date_from_bq(ti):
   sql = "SELECT max(payment_date) FROM bi_department.transaction_table"
   bq_hook = BigQueryHook(gcp_conn_id="gcp_dvd_bi",
                          use_legacy_sql=True,
                          delegate_to=None,
                          location=LOCATION)
   result = bq_hook.get_records(sql)
   unix_date = int(result[0][0])
   date = datetime.utcfromtimestamp(unix_date).strftime('%Y-%m-%d %H:%M:%S')
   ti.xcom_push(key="date", value=date)

# ------------ DAG --------------
   
with DAG(dag_id='sync_transactions_to_bq', 
         catchup= False, 
         schedule=None,
         start_date=datetime(2024, 2, 1)
         ) as dag:
   
   # Load the last transaction update date from BigQuery
   get_max_date_id = PythonOperator(task_id="get_max_date_bq",
                                 python_callable=get_max_date_from_bq)
   
   # Load the new transactions from Postgres and save them to GCS
   new_transactions_to_gstorage = PostgresToGCSOperator(task_id = "new_transactions_to_gstorage", 
                                                        postgres_conn_id="postgres_dvd_rental", 
                                                        gcp_conn_id = 'gcp_dvd_bi',
                                                        bucket = BUCKET,
                                                        filename = STORAGE_DOC,
                                                        export_format='csv',
                                                        use_server_side_cursor=False,
                                                        sql = './latest_transactions.sql'                                                        
                                                        )
   transfer_from_storage_to_bq = GCSToBigQueryOperator(task_id="from_gcs_to-bq", 
                                                       bucket=BUCKET, 
                                                       gcp_conn_id = 'gcp_dvd_bi',
                                                       source_objects=STORAGE_DOC, 
                                                       destination_project_dataset_table = 'bi-dvd.bi_department.transaction_table', 
                                                       create_disposition='CREATE_IF_NEEDED', 
                                                       write_disposition='WRITE_APPEND',
                                                       skip_leading_rows=1,
                                                       allow_quoted_newlines=True
                                                       )
   # TODO - add storage cleanup 
   
get_max_date_id >> new_transactions_to_gstorage >> transfer_from_storage_to_bq

