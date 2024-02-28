import numpy as np
import pandas as pd

from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook

CSV_LOCATION = '/tmp/data.csv'

def load_exisiting_users(ti):
    hook = PostgresHook(postgres_conn_id="postgres_dvd_rental")
    df = hook.get_pandas_df(sql="""
            SELECT 
            p.*,
            r.inventory_id
        FROM payment AS p
        JOIN rental AS r on p.rental_id = r.rental_id       
        """)
    df.to_csv(CSV_LOCATION, index=None)

def create_transaction():
    df = pd.read_csv(CSV_LOCATION)
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
    payment_date = datetime.now()
    payment_row['payment_date'] = payment_date.strftime('%Y-%m-%d %H:%M:%S')

    # rental date
    # generate rental duration in days between 1 and 10 days
    rental_days = np.random.randint(1, 11) 
    rental_date = payment_date - timedelta(days=rental_days)
    rental_row['rental_date'] = rental_date.strftime('%Y-%m-%d %H:%M:%S')

    # return date
    return_date = datetime.now()
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


    payment_df = pd.DataFrame(
        data ={"payment_id": payment_row['payment_id'], 
               "customer_id": payment_row['customer_id'], 
               "staff_id": payment_row['staff_id'], 
               "rental_id": payment_row['rental_id'], 
               "amount": payment_row['amount'], 
               "payment_date": payment_row['payment_date']},
        index =[0]
            
    )


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
    

def insert_payment_transaction():
    hook = PostgresHook(postgres_conn_id="postgres_dvd_rental")
    hook.copy_expert(
    sql="COPY payment FROM stdin WITH DELIMITER AS ','",
    filename='/tmp/payment_row.csv'
    )
    
def insert_rental_transaction():
    hook = PostgresHook(postgres_conn_id="postgres_dvd_rental")
    hook.copy_expert(
    sql="COPY rental FROM stdin WITH DELIMITER AS ','",
    filename='/tmp/rental_row.csv'
    )
