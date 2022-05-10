import datetime
from airflow.decorators import dag, task

@dag(
    description="US Treasury Auction ETL Process",
    schedule_interval=datetime.timedelta(days=1),
    start_date=datetime.datetime.now(),
    catchup=True,
    tags=['treasury', 'yields', 'bonds']
)
def us_treasury_etl_process():
    """
    ## US Treasury Auction ETL Process

    """
    @task
    def extract():
        return 1

    @task(multiple_outputs=True)
    def transform(data):
        return data
    
    @task
    def load(data):
        print(data)
    
    data = extract()
    data_transformed = transform(data)
    load(data_transformed)
us_treasury_etl = us_treasury_etl_process()