<img src="images/header_image_readmeheader.jpg"
     alt="Markdown Monster icon"
     style="margin: 0px;" />

# Airflow Pipeline for Movie Rental Analytics

In this project, inspired by the Bar Dadon's [tutorial](https://towardsdev.com/data-engineering-project-bi-department-part-1-cb6086da3662), I used a PostgreSQL sample [database](https://www.postgresqltutorial.com/postgresql-getting-started/postgresql-sample-database/) to emulate a DVD Rental Business. The ERD for the database is shown below.

<img src="images/dvd-rental-sample-database-diagram.png"
     alt="Markdown Monster icon"
     style="margin: 0px;" />

There are two DAG files that I created for this project. First DAG - [generate_transations.py](dags/generate_transaction.py) is used to emulate real-world transactions. The second DAG [push_latest_to_bq.py](dags/push_latest_to_bq.py) is for loading historic transaction data from PostgreSQL into BigQuery Warehouse/Data Mart for data analysis.  

 