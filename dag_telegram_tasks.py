from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def analyze_results():
    import get_telegram_tasks
    get_telegram_tasks.analyze_results()


# At 06:00 on every day-of-week from Monday through Friday.
dag = DAG(
    dag_id="telegram_tasks",
    description="Выгрузка и анализ заявок ТП ПОКБ в Телеграмме",
    start_date=datetime(2023, 1, 1),
    schedule_interval="0 6 * * 2-6",
    catchup=False,
)

python_task = PythonOperator(
    task_id="run_processing",
    python_callable=analyze_results,
    provide_context=True,
    dag=dag,
)
