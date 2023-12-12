from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def analyze_results():
    import analyze_phone_calls
    analyze_phone_calls.analyze_results()


def download_report():
    import get_phone_calls
    get_phone_calls.start_download_phone_calls()


dag = DAG(
    dag_id="analyze_phone_calls",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
)


python_task_download = PythonOperator(
    task_id="get_phone_calls_from_web",
    python_callable=download_report,
    provide_context=True,
    dag=dag,
)

python_task_analyze = PythonOperator(
    task_id="run_phone_calls_processing",
    python_callable=analyze_results,
    provide_context=True,
    dag=dag,
)

python_task_download >> python_task_analyze
