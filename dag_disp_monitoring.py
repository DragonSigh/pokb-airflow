from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def start_mysql_export():
    import get_disp_monitoring
    get_disp_monitoring.start_mysql_export()


default_args = {
    'start_date': datetime(2023, 1, 1),
    'sla': timedelta(minutes=60)
}

# At 07:30 on every day
dag = DAG(
    dag_id="disp_monitoring",
    description="Выгрузка в дашборд мониторинга диспансеризации",
    schedule_interval="30 7 * * *",
    catchup=False,
    default_args=default_args
)

python_task = PythonOperator(
    task_id="run_mysql_export",
    python_callable=start_mysql_export,
    provide_context=True,
    dag=dag,
)
