# import packages

import os
import logging
import pandas as pd

from datetime import datetime, timedelta

from pendulum import local
from sqlalchemy import create_engine


from google.cloud import storage

from airflow import DAG
from airflow.utils.dates import days_ago

# Import our operators
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator



dataset_file = "01-01-2021.csv"
dataset_url= f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
data_path = f"{path_to_local_home}/{dataset_file}"


# create function that sends downloaded data to postgresdb

def local_to_postgres(src_file):

    engine = create_engine(f'postgresql://root:root@pg-database:5432/covid_db')    

    df = pd.read_csv(src_file) 

    df.head(n=0).to_sql(name="covid_data", con=engine, if_exists='replace')

    df.to_sql(name="covid_data", con=engine, if_exists='append')


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
    # 'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id = 'covid_pipe',
    default_args=default_args,
    description='Schedule Covid data ingestion',
    schedule_interval='@daily',
    start_date=days_ago(6),
    catchup=True
) as dag:

    download_data= BashOperator(
        task_id="download_data",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    local_to_db = PythonOperator(
        task_id = "local_to_postgres",
        python_callable = local_to_postgres,
        op_kwargs = {'src_file':data_path}

    )
    remove_data = BashOperator(
        task_id="remove_data",
        bash_command= f"rm {data_path}"
    )


    download_data >> local_to_db >> remove_data















