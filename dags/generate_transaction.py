from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import datetime

import pandas as pd
import numpy as np

#executables
def _create_transaction(ti):
    df = pd.read_csv("/tmp/data.csv")
    df['payment_date'] = pd.to_datetime(df['payment_date'])
    df['payment_date'] = df['payment_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    payment_row = {}
    rental_row = {}

    # Generate payment id
    payment_id = df.payment_id.max() + 1
    payment_row['payment_id'] = payment_id

    # customer id
    customer_id = np.random.randint(low=df.customer_id.min(), high=df.customer_id.max())
    payment_row['customer_id'] = customer_id
    rental_row['customer_id'] = customer_id

    # staff id
    staff_id = np.random.randint(low=df.staff_id.min(), high=df.staff_id.max())
    payment_row['staff_id'] = staff_id
    rental_row['staff_id'] = staff_id

    # rental id
    rental_id = df.rental_id.max() + 1
    payment_row['rental_id'] = rental_id
    rental_row['rental_id'] = rental_id

    # payment date-current date: format:2007-02-15 22:25:46.996577
    payment_date = datetime.datetime.now()
    payment_row['payment_date'] = payment_date.strftime('%Y-%m-%d %H:%M:%S')

    # rental date
    rental_date = payment_date - datetime.timedelta(days=1)
    rental_row['rental_date'] = rental_date.strftime('%Y-%m-%d %H:%M:%S')

    # return date
    return_date = datetime.datetime.now()
    rental_row['return_date'] = return_date.strftime('%Y-%m-%d %H:%M:%S')

    # last update
    last_update = payment_date
    rental_row['last_update'] = last_update.strftime('%Y-%m-%d %H:%M:%S')

    # amount
    amount = np.random.randint(low=1, high=10)
    payment_row['amount'] = amount

    # inventory
    inventory_id = np.random.randint(low=df.inventory_id.min(), high=df.inventory_id.max())
    rental_row['inventory_id'] = inventory_id

    # payment_df = pd.DataFrame(
    #     data = np.array([[payment_row['payment_id'], payment_row['customer_id'], payment_row['staff_id'], payment_row['rental_id'], payment_row['amount'], payment_row['payment_date']]]),
    #     columns =["payment_id", "customer_id", "staff_id", "rental_id", "amount", "payment_date"]
    # )

    payment_df = pd.DataFrame(
        data ={"payment_id": payment_row['payment_id'], 
               "customer_id": payment_row['customer_id'], 
               "staff_id": payment_row['staff_id'], 
               "rental_id": payment_row['rental_id'], 
               "amount": payment_row['amount'], 
               "payment_date": payment_row['payment_date']},
        index =[0]
            
    )

    # rental_df = pd.DataFrame(
    #     data = np.array([[rental_row['rental_id'], rental_row['rental_date'], rental_row['inventory_id'], rental_row['customer_id'], rental_row['return_date'], rental_row['staff_id'], rental_row['last_update']]]),
    #     columns = ["rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "staff_id", "last_update"]
    # )
    rental_df = pd.DataFrame(
        data = {"rental_id":rental_row['rental_id'], 
                   "rental_date": rental_row['rental_date'], 
                   "inventory_id": rental_row['inventory_id'], 
                   "customer_id": rental_row['customer_id'], 
                   "return_date": rental_row['return_date'], 
                   "staff_id": rental_row['staff_id'], 
                   "last_update": rental_row['last_update']},
        index = [0]
    )
    payment_df.to_csv('/tmp/payment_row.csv', index=None, header=False)
    rental_df.to_csv('/tmp/rental_row.csv', index=None, header=False)
    

def _insert_payment_transaction(ti):
    hook = PostgresHook(postgres_conn_id="postgres_dvd_rental")
    hook.copy_expert(
    sql="COPY payment FROM stdin WITH DELIMITER AS ','",
    filename='/tmp/payment_row.csv'
    )
    
def _insert_rental_transaction(ti):
    hook = PostgresHook(postgres_conn_id="postgres_dvd_rental")
    hook.copy_expert(
    sql="COPY rental FROM stdin WITH DELIMITER AS ','",
    filename='/tmp/rental_row.csv'
    )

def _load_exisiting_users(ti):
    hook = PostgresHook(postgres_conn_id="postgres_dvd_rental")
    df = hook.get_pandas_df(sql="""
            SELECT 
            p.*,
            r.inventory_id
        FROM payment AS p
        JOIN rental AS r on p.rental_id = r.rental_id       
        """)
    df.to_csv('/tmp/data.csv', index=None)




with DAG(dag_id="transaction_generator", 
         schedule_interval='@hourly',
         start_date=datetime.datetime(2024,2,17),
         catchup=False,
         default_args={"owner": "JD"}) as dag:
    
    create_transaction = PythonOperator(
        task_id="create_transaction",
        python_callable=_create_transaction
    )

    load_existing_users = PythonOperator(
        task_id="load_exisitng_users",
        python_callable = _load_exisiting_users
    )
    
    insert_payment = PythonOperator(
        task_id = "insert_payment",
        python_callable = _insert_payment_transaction
    )

    insert_rental = PythonOperator(
        task_id = "insert_rental",
        python_callable = _insert_rental_transaction
    )
    
    load_existing_users >> create_transaction >> insert_rental >> insert_payment