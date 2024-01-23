from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def check_metric_007():
    import get_metric_007

    get_metric_007.check_metric_007()


def notify_tg_channel_on_success(context):
    import metrics_collector.telegram as telegram

    text = "🟢 Отчёт по Показателю 7 успешно сформирован:"
    link = r"`\\\\10.2.14.224\\share\\download\\Показатель 7`"
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
