from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def start_mysql_export():
    import get_disp_monitoring

    get_disp_monitoring.start_mysql_export()


def notify_tg_channel_on_success(context):
    import metrics_collector.telegram as telegram

    text = "🟢 Отчёт по мониторингу диспансеризации успешно сформирован:"
    link = r"`https://docs.google.com/spreadsheets/d/17U0jjvNCvrqLbu3MT5aiI4FCh4ChAV5B_7PT302aKhE/edit?usp=sharing`"
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
    log_link = telegram.escape_markdown(
        last_task.log_url.replace("localhost", "10.2.14.224")
    )
    text = f"🔴 Ошибка при выполнении задачи *[{task_name}]({log_link})*"
    date = datetime.now().ctime()
    msg_parts = {"Дата": date}
    msg = "\n".join(
        [text, *[f"*{key}*: {value}" for key, value in msg_parts.items()]]
    ).strip()
    telegram.send_telegram_message(telegram.ERRORS_CHAT_ID, msg)


default_args = {
    "start_date": datetime(2023, 1, 1),
    "sla": timedelta(minutes=60),
    "retries": 5,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "on_success_callback": None,
    "on_failure_callback": alert_tg_channel_on_error,
}

# At 07:30 on every day
dag = DAG(
    dag_id="disp_monitoring",
    description="Выгрузка в дашборд мониторинга диспансеризации",
    schedule_interval="30 7 * * *",
    catchup=False,
    default_args=default_args,
)

python_task = PythonOperator(
    task_id="run_mysql_export",
    python_callable=start_mysql_export,
    provide_context=True,
    on_success_callback=notify_tg_channel_on_success,
    dag=dag,
)
