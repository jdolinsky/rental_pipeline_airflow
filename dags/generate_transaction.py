from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

#executables
import scripts.create_transaction as sct


with DAG(dag_id="transaction_generator", 
         description="Transaction simulator. Creates a transactioin record in the DB.",
         schedule_interval=None,
         start_date=datetime(2024,2,20),
         catchup=False,
         default_args={
             "email": ["dervish-tankful-0f@icloud.com"],
             "email_on_failure": True,
             "retries": 1,
             "retry_delay": timedelta(minutes=3),
             "owner": "JD"
             }
         ) as dag:
    
    create_transaction = PythonOperator(
        task_id="create_transaction",
        python_callable=sct.create_transaction
    )

    load_existing_users = PythonOperator(
        task_id="load_exisitng_users",
        python_callable = sct.load_exisiting_users
    )
    
    insert_payment = PythonOperator(
        task_id = "insert_payment",
        python_callable = sct.insert_payment_transaction
    )

    insert_rental = PythonOperator(
        task_id = "insert_rental",
        python_callable = sct.insert_rental_transaction
    )
    
    load_existing_users >> create_transaction >> insert_rental >> insert_payment