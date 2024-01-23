from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def check_metric_007():
    import get_metric_007

    get_metric_007.check_metric_007()


def notify_tg_channel_on_success(context):
    import metrics_collector.telegram as telegram

    text = r"Отчёт по Показателю 7 успешно сформирован:"
    link = r"`\\\\10.2.14.224\\share\\download\\Показатель 7`"
    date = datetime.now().ctime()
    telegram.send_telegram_message(telegram.ANALYTICS_CHAT_ID, f"{text} {link} {date}")


def alert_tg_channel_on_error(context):
    import metrics_collector.telegram as telegram

    last_task = context.get("task_instance")
    task_name = telegram.escape_markdown(last_task.task_id)
    #log_link = f"({last_task.log_url})[{task_name}]>"
    error_message = context.get("exception") or context.get("reason")
    #execution_date = context.get("execution_date")
    title = f"Ошибка в {task_name}!"
    #msg_parts = {"Дата": execution_date, "Лог": log_link, "Ошибка": error_message}
    telegram.send_telegram_message(
        telegram.ERRORS_CHAT_ID, f"🔴 Ошибка в задаче {task_name} " + telegram.escape_markdown(last_task.log_url)
    )


default_args = {
    "start_date": datetime(2023, 1, 1),
    "sla": timedelta(minutes=60),
    "on_success_callback": notify_tg_channel_on_success,
    "on_failure_callback": alert_tg_channel_on_error,
}

# At 15:00 on Wednesday.
dag = DAG(
    dag_id="metric_007",
    description="Выгрузка и анализ Показателя 07",
    schedule="0 15 * * 3",
    catchup=False,
    default_args=default_args,
)

check_metric_task = PythonOperator(
    task_id="run_metric_007",
    python_callable=check_metric_007,
    dag=dag,
)
