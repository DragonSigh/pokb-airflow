from airflow.models import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

import get_phone_calls

dag = DAG(
    dag_id="analyze_phone_calls",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
)

sensor_task = FileSensor(
    task_id="sense_phone_calls_file",
    filepath=get_phone_calls.UPLOAD_FILE_PATH,
    fs_conn_id="fs_default",
    poke_interval=60,
    dag=dag,
)

python_task = PythonOperator(
    task_id="run_phone_calls_processing",
    python_callable=get_phone_calls.upload_results,
    provide_context=True,
    dag=dag,
)


sensor_task >> python_task
