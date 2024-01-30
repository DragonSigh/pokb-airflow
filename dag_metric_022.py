from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def check_metric_022():
    import get_metric_022
    get_metric_022.check_metric_022()


def notify_tg_channel_on_success(context):
    import metrics_collector.telegram as telegram

    text = "🟢 Отчёт по Показателю 22 успешно сформирован:"
    link = r"`\\\\10.2.14.224\\share\\download\\Показатель 22`"
    date = datetime.now().ctime()
    msg_parts = {"Дата": date, "Ссылка": link}
    msg = "\n".join(
        [text, *[f"*{key}*: {value}" for key, value in msg_parts.items()]]
    ).strip()
    telegram.send_telegram_message(telegram.ANALYTICS_CHAT_ID, msg)


def alert_tg_channel_on_error(context):
    import metrics_collector.telegram as telegram

    last_task = context.get("task_instance")
    task_name = telegram.escape_markdown(last_task.task_id)
    log_link = telegram.escape_markdown(last_task.log_url.replace("localhost", "10.2.14.224"))
    # execution_date = context.get("execution_date")
    text = f"🔴 Ошибка при выполнении задачи *[{task_name}]({log_link})*"
    date = datetime.now().ctime()
    msg_parts = {"Дата": date}
    msg = "\n".join(
        [text, *[f"*{key}*: {value}" for key, value in msg_parts.items()]]
    ).strip()
    telegram.send_telegram_message(
        telegram.ERRORS_CHAT_ID, msg
    )


default_args = {
    'start_date': datetime(2023, 1, 1),
    'sla': timedelta(minutes=60),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
    "on_success_callback": None,
    "on_failure_callback": alert_tg_channel_on_error,
}

# At 10:00 on every day-of-week from Monday through Friday.
dag = DAG(
    dag_id="metric_022",
    description="Выгрузка и анализ Показателя 22",
    schedule="0 10 * * 1-5",
    catchup=False,
    default_args=default_args
)

check_metric_task = PythonOperator(
    task_id="run_metric_022",
    python_callable=check_metric_022,
    on_success_callback=notify_tg_channel_on_success,
    dag=dag,
)
