import json
import datetime
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.http_operator import SimpleHttpOperator


@dag(
    description="Test DAG Operator ETL Process",
    schedule_interval=datetime.timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['test', 'etl']
)
def dag_with_operator_etl_process():
    get_api_results_task = SimpleHttpOperator(
        task_id="get_api_results",
        method='GET',
        endpoint="/api/people/1",
        headers={"Content-Type": "application/json"},
        do_xcom_push=True,
        http_conn_id="http",
    )


    @task
    def parse_results(api_results):
        result = json.loads(api_results)
        print(result)
        return result


    parsed_results = parse_results(api_results=get_api_results_task.output)

dag_with_operator_etl = dag_with_operator_etl_process()