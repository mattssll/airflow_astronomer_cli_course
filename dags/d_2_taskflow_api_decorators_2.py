from airflow.decorators import dag, task
from datetime import datetime, timedelta
from typing import Dict
import requests
import logging

# Get current value of bitcoin in U$D with a 24 hours change percentage
API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"


@dag(schedule_interval='@daily', start_date=datetime(2021, 12, 1), catchup=False, dagrun_timeout = timedelta(minutes=30))
def d_taskflow_fetch_api_example():

    @task(task_id='extract', retries=2)
    def extract_bitcoin_price() -> Dict[str, float]:
        return requests.get(API).json()['bitcoin']

    @task(multiple_outputs=True)
    def process_data(response: Dict[str, float]) -> Dict[str, float]:
        logging.info(response)
        print("I'm a print, and this is the difference between a log and a print")
        return {'usd': response['usd'], 'change': response['usd_24h_change']}

    @task
    def store_data(data: Dict[str, float]):
        logging.info(f"Store: {data['usd']} with change {data['change']}")

    store_data(process_data(extract_bitcoin_price()))

dag = d_taskflow_fetch_api_example()