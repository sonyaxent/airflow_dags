import pandas as pd
import io
import pprint
import requests
import json

from datetime import datetime
from airflow import DAG

default_args = {'owner': 'admin',
                'start_date': datetime(2021, 1, 1, 1, 1)
                }

with DAG(dag_id='etl_dag',
         schedule_interval=None,
         default_args=default_args) as dag:
    @dag.task()
    def get_data() -> json:
        url = "https://data.bloomington.in.gov/dataset/117733fb-31cb-480a-8b30-fbf425a690cd/resource/8673744e-53f2-42d1-9d05-4e412bd55c94/download/monroe-county-crash-data2003-to-2015.csv"
        raw_csv = requests.get(url).content
        df = pd.read_csv(io.StringIO(raw_csv.decode('windows-1252')))
        return df.to_json()


    @dag.task()
    def get_result(json_data: json) -> json:
        return pd.read_json(json_data).groupby('Year').size().to_json()


    @dag.task()
    def print_result(result_series: json) -> None:
        pprint.pprint(result_series)


    cars_crash_df = get_data()
    result = get_result(cars_crash_df)
    print_result(result)
