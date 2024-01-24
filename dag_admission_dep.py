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


def notify_tg_channel_on_success(context):
    import metrics_collector.telegram as telegram

    text = "🟢 Отчёт по дашборду приёмных отделений успешно сформирован:"
    link = r"`\\\\10.2.14.224\\share\\download\\Приемные отделения`"
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
    "on_success_callback": None,
    "on_failure_callback": alert_tg_channel_on_error,
}

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
    on_success_callback=notify_tg_channel_on_success,
    dag=dag,
)

bi_export_task >> analyze_task
hospital_export_task >> analyze_task
