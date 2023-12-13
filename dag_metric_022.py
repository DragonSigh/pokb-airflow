from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def check_metric_022():
    import get_metric_022
    get_metric_022.check_metric_022()


default_args = {
    'start_date': datetime(2023, 1, 1),
    'sla': timedelta(minutes=60)
}

dag = DAG(
    dag_id="metric_022",
    description="Выгрузка и анализ Показаталея 22",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args
)

python_task = PythonOperator(
    task_id="run_metric_022",
    python_callable=check_metric_022,
    provide_context=True,
    dag=dag,
)
