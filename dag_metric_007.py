from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def check_metric_007():
    import get_metric_007
    get_metric_007.check_metric_007()


default_args = {
    'start_date': datetime(2023, 1, 1),
    'sla': timedelta(minutes=60)
}

# At 15:00 on Thursday.
dag = DAG(
    dag_id="metric_007",
    description="Выгрузка и анализ Показателя 07",
    schedule_interval="0 15 * * 4",
    catchup=False,
    default_args=default_args
)

python_task = PythonOperator(
    task_id="run_metric_007",
    python_callable=check_metric_007,
    provide_context=True,
    dag=dag,
)
