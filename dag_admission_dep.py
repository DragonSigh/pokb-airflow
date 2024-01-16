from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def start_hospital_export():
    import get_admission_dep
    get_admission_dep.start_hospital_export()


def start_bi_export():
    import get_admission_dep
    get_admission_dep.start_bi_export()


def start_analyze():
    import get_admission_dep
    get_admission_dep.start_analyze()


default_args = {
    'start_date': datetime(2023, 1, 1),
    'sla': timedelta(minutes=60)
}

# At hour 9 on every day-of-week from Monday through Friday
dag = DAG(
    dag_id="hospital_admission_dep",
    description="Анализ отчетов приемного отделения",
    schedule_interval="0 9 * * 1-5",
    catchup=False,
    default_args=default_args
)

hospital_export_task = PythonOperator(
    task_id="hospital_export",
    python_callable=start_hospital_export,
    provide_context=True,
    dag=dag,
)

bi_export_task = PythonOperator(
    task_id="bi_export",
    python_callable=start_bi_export,
    provide_context=True,
    dag=dag,
)

analyze_task = PythonOperator(
    task_id="analyze",
    python_callable=start_analyze,
    provide_context=True,
    dag=dag,
)

shut_down = PythonOperator(
    task_id="shut_down",
    python_callable=shut_down,
    provide_context=True,
    dag=dag,
)

bi_export_task >> analyze_task
hospital_export_task >> analyze_task
analyze_task >> shut_down
