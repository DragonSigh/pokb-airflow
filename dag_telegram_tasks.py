from airflow.models import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

import get_telegram_tasks

dag = DAG(
    dag_id="analyze_telegram_tasks",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
)

sensor_task = FileSensor(
    task_id="sense_file",
    filepath=get_telegram_tasks.UPLOAD_FILE_PATH,
    fs_conn_id="fs_default",
    poke_interval=60,
    dag=dag,
)

python_task = PythonOperator(
    task_id="run_processing",
    python_callable=get_telegram_tasks.analyze_results,
    provide_context=True,
    dag=dag,
)


sensor_task >> python_task
