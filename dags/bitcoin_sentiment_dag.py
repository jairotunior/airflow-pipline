import json
import datetime
import pandas as pd
import requests
from airflow.decorators import dag, task


@dag(
    description="Bitcoin Sentiment ETL Process",
    schedule_interval=datetime.timedelta(days=1),
    start_date=datetime.datetime.now(),
    catchup=True,
    tags=['sentiment']
)
def bitcoin_sentiment_etl_process():
    """
    ## Bitcoin Sentiment ETL Process
    This ETL process extract the Bitcoin Sentiment.

    """
    @task
    def extract():
        limit = 0
        date_format = "world"
        fng_bitcoin_base_url = f"https://api.alternative.me/fng/?limit={limit}&format=json&date_format={date_format}"
       
        data = None

        try:
            response = requests.get(fng_bitcoin_base_url)

            if response.status_code == 200:
                data = response.json()['data'][0]
        except:
            return data

        return data

    @task(multiple_outputs=True)
    def transform(data):
        return {'sentiment': data['value'], 'time': data['timestamp']}
    
    @task
    def load(data):
        print('Sentiment Bitcoin: {} at {}'.format(data['sentiment'], data['time']))
    
    data = extract()
    data_transformed = transform(data)
    load(data_transformed)

bitcoin_sentiment_etl = bitcoin_sentiment_etl_process()