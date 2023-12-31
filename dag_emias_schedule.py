from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def start_mysql_export():
    import get_emias_schedule
    get_emias_schedule.start_mysql_export()


default_args = {
    'start_date': datetime(2023, 1, 1),
    'sla': timedelta(minutes=60)
}

# At hour 8, 12, and 20 on every day-of-week from Monday through Friday.
dag = DAG(
    dag_id="emias_schedule",
    description="Выгрузка расписания из ЕМИАС в дашборд",
    schedule_interval="0 8,12,20 * * 1-5",
    catchup=False,
    default_args=default_args
)

python_task = PythonOperator(
    task_id="run_mysql_export",
    python_callable=start_mysql_export,
    provide_context=True,
    dag=dag,
)
