from airflow.models import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

import get_metric_022

dag = DAG(
    dag_id="analyze_metric_022",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
)

python_task = PythonOperator(
    task_id="run_metric_022",
    python_callable=get_metric_022.check_metric_022,
    provide_context=True,
    dag=dag,
)
