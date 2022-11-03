import pandas as pd

import time

import os

import argparse

#Create SQL connection
from sqlalchemy import create_engine

def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name 
    url = params.url

    csv_name = 'output.csv'
    
    os.system(f"wget {url} -O {csv_name}") 
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')    


    df = pd.read_csv(csv_name)  

    
    #t_start = time()

    # df.drop(['FIPS', 'Admin2', 'Province_State'], axis=1, inplace=True) 

    # df.dropna(inplace=True) 

    df['Last_Update'] = pd.to_datetime(df['Last_Update'])

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    
    df.to_sql(name=table_name, con=engine, if_exists='append')
    #t_end = time()
    
    print("Done")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the parquet file')

    args = parser.parse_args()

    main(args)
