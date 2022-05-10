import datetime
from airflow.decorators import dag, task

@dag(
    schedule_interval=datetime.timedelta(hours=1),
    description="Get CNN Sentiment from Equity Markets",
    start_date=datetime.datetime.today(),
    catchup=False,
    tags=['sentiment'],
)
def cnn_sentiment_etl_process():
    """
    ### CNN Sentiment ETL Process
    This is a simple ETL data pipeline extract information about the sentiment published
    by CNN.
    [here](https://edition.cnn.com/markets/fear-and-greed)
    """
    @task
    def extract():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        return 10

    @task(multiple_outputs=True)
    def transform(sentiment):
        return {'sentiment': sentiment}

    @task
    def load(sentiment):
        print(f"CNN Sentiment Value: {sentiment}")

    data = extract()
    data_transformed = transform(data)
    load(data_transformed['sentiment'])
cnn_sentiment_etl = cnn_sentiment_etl_process()