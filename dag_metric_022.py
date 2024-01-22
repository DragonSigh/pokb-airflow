from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def check_metric_022():
    import get_metric_022
    get_metric_022.check_metric_022()


def send_message_run():
    import metrics_collector.telegram as telegram
    text = r"Отчёт по Показателю 22 успешно сформирован:"
    link = r"`\\\\10.2.14.224\\share\\download\\Показатель 22`"
    telegram.send_telegram_message(telegram.ANALYTICS_CHAT_ID, f"{text}  {link}")


default_args = {
    'start_date': datetime(2023, 1, 1),
    'sla': timedelta(minutes=60)
}

# At 07:00 on every day-of-week from Tuesday through Friday.
dag = DAG(
    dag_id="metric_022",
    description="Выгрузка и анализ Показателя 22",
    schedule="0 7 * * 2-5",
    catchup=False,
    default_args=default_args
)

check_metric_task = PythonOperator(
    task_id="run_metric_022",
    python_callable=check_metric_022,
    dag=dag,
)


send_message = PythonOperator(
    task_id="send_message",
    python_callable=send_message_run,
    provide_context=True,
    dag=dag,
)

check_metric_task >> send_message
