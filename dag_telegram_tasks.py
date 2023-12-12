from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def analyze_results():
    import get_telegram_tasks
    get_telegram_tasks.analyze_results()


dag = DAG(
    dag_id="analyze_telegram_tasks",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
)

python_task = PythonOperator(
    task_id="run_processing",
    python_callable=analyze_results,
    provide_context=True,
    dag=dag,
)
