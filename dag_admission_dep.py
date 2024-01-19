from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def start_hospital_export_run():
    from get_admission_dep import start_hospital_export
    start_hospital_export()


def start_bi_export_run():
    from get_admission_dep import start_bi_export
    start_bi_export()


def start_analyze_run():
    from get_admission_dep import start_analyze
    start_analyze()


def send_message_run():
    import metrics_collector.telegram as telegram

    text = r"Отчёт по дашборду приёмных отделений успешно сформирован"
    link = r"`\\10.2.14.224\share\download\Приемные отделения`"
    telegram.send_telegram_message(telegram.ANALYTICS_CHAT_ID, f"{text}: {link}")


default_args = {"start_date": datetime(2023, 1, 1), "sla": timedelta(minutes=60)}

# At hour 9 on every day-of-week from Monday through Friday
dag = DAG(
    dag_id="hospital_admission_dep",
    description="Анализ отчетов приемного отделения",
    schedule_interval="55 8 * * 1-5",
    catchup=False,
    default_args=default_args,
)

hospital_export_task = PythonOperator(
    task_id="hospital_export",
    python_callable=start_hospital_export_run,
    provide_context=True,
    dag=dag,
)

bi_export_task = PythonOperator(
    task_id="bi_export",
    python_callable=start_bi_export_run,
    provide_context=True,
    dag=dag,
)

analyze_task = PythonOperator(
    task_id="analyze",
    python_callable=start_analyze_run,
    provide_context=True,
    dag=dag,
)

send_message = PythonOperator(
    task_id="send_message",
    python_callable=send_message_run,
    provide_context=True,
    dag=dag,
)

bi_export_task >> analyze_task
hospital_export_task >> analyze_task
analyze_task >> send_message
