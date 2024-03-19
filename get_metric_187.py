import metrics_collector.config as config
import metrics_collector.utils as utils
import metrics_collector.bi_emias as bi_emias
from datetime import date, timedelta
import json
import pandas as pd
from sqlalchemy import create_engine
import logging
import os

# Настройки
FULL_PATH_TO_SCRIPT = os.path.abspath(__file__)
SCRIPT_DIRECTORY = str(os.path.dirname(FULL_PATH_TO_SCRIPT))
PATH_TO_CREDENTIAL = r"/home/user/"
EXPORT_PATH = r"/etc/samba/share/download/Показатель 187"
SQL_QUERY_FILE_PATH = SCRIPT_DIRECTORY + r"/metrics_collector/pokb_get_schedule.sql"
PATH_TO_MYSQL_CREDENTIAL = r"/home/user/auth-mysql.json"

first_date = config.first_date
last_date = config.last_date
yesterday_date = config.yesterday_date

pd.set_option("max_colwidth", 120)
pd.set_option("display.width", 500)


def start_bi_report_saving():
    logging.info("Начата авторизация в BI ЕМИАС")
    # Чтение данных для авторизации
    with open(PATH_TO_CREDENTIAL) as f:
        data = json.load(f)

    bi_emias.authorize(data["username"], data["password"])

    # Выгрузка отчета
    bi_emias.load_any_report("palliativ_kr_lasttap_mm", False)
    bi_emias.export_report()
    logging.info("Выгрузка из BI ЕМИАС завершена")


def start_mysql_export():
    with open(SQL_QUERY_FILE_PATH, "r", encoding="utf-8") as file:
        sql_query = file.read()

    with open(PATH_TO_MYSQL_CREDENTIAL) as f:
        data = json.load(f)

    conf_server = data["conf_server"]
    conf_port = data["conf_port"]
    conf_database = data["conf_database"]
    conf_username = data["conf_username"]
    conf_password = data["conf_password"]

    # Create a connection string using SQLAlchemy
    engine = create_engine(
        f"mssql+pymssql://{conf_username}:{conf_password}@{conf_server}:{conf_port}/{conf_database}?charset=utf8"
    )

    df = pd.read_sql_query(sql_query, engine)

    df = df.map(
        lambda x: x.encode("latin1").decode("cp1251") if isinstance(x, str) else x
    )

    # Делаем дату датой
    #df["date_close"] = pd.to_datetime(df["date_close"], format="%Y-%m-%d").dt.date
    print(config.reports_path)
    df.to_csv(os.path.join(config.reports_path, "taps.csv"))


def analyze_187_data():
    # Загружаем отчёт в датафрейм
    # http://bi.mz.mosreg.ru/#form/palliativ_kr_lasttap_mm
    # Наблюдение паллиативного пациента не менее 1 раза в месяц  (показатель КР 187)
    df_pall = pd.read_excel(
        os.path.join(
            config.reports_path,
            "Наблюдение паллиативного пациента не менее 1 раза в месяц  (показатель КР 187).xlsx",
        ),
        skiprows=1,
        header=0,
    )

    # MySQL скрипт
    df_taps = pd.read_csv(os.path.join(config.reports_path, "taps.csv"), engine="python", header=0)

    df_taps = df_taps.drop_duplicates(
        subset=["last_name", "first_name", "middle_name", "birth_year"], keep="first"
    )

    # Только Подольская ОКБ
    df_pall = df_pall[(df_pall["ОГРН"] == 1215000036305)]

    df_pall["Дата последнего ТАПа"] = pd.to_datetime(
        df_pall["Дата последнего ТАПа"], format="%d.%m.%Y %H:%M:%S"
    ).dt.date
    df_pall["Отделение"] = df_pall["Подразделение"]
    df_pall["Подразделение"] = df_pall["Подразделение"].apply(utils.get_department)

    df_pall = df_pall[
        [
            "Подразделение",
            "Отделение",
            "Фамилия пациента",
            "Имя пациента",
            "Отчество пациента",
            "Год рождения пациента",
            "Возраст пациента",
            "Дата последнего ТАПа",
        ]
    ]

    df_pall = df_pall.merge(
        df_taps,
        how="left",
        left_on=[
            "Фамилия пациента",
            "Имя пациента",
            "Отчество пациента",
            "Год рождения пациента",
        ],
        right_on=["last_name", "first_name", "middle_name", "birth_year"],
    )

    df_pall["tap_date"] = pd.to_datetime(df_pall["tap_date"], format="%Y-%m-%d").dt.date

    df_pall = df_pall.sort_values(by="tap_date")

    df_pall = df_pall[
        [
            "Подразделение",
            "Отделение",
            "mkab_number",
            "Фамилия пациента",
            "Имя пациента",
            "Отчество пациента",
            "Год рождения пациента",
            "Возраст пациента",
            "tap_date",
        ]
    ]

    df_pall.columns = [
        "Подразделение",
        "Отделение",
        "Номер МКАБ",
        "Фамилия",
        "Имя",
        "Отчество",
        "Год рождения",
        "Возраст",
        "Дата последнего ТАПа в ЕМИАС",
    ]

    # styler = df_pall.style
    # styler = styler.map(highlight_patients, subset=["tap_date"])
    # styler.to_excel(metric_path + "\\result.xlsx")

    for department in df_pall["Подразделение"].unique():
        df_temp = df_pall[df_pall["Подразделение"] == department].drop(
            ["Подразделение"], axis=1
        )
        df_temp = df_temp.sort_values(by="Дата последнего ТАПа в ЕМИАС")
        # Фильтрация датафрейма по уникальному значению в колонке
        styler = df_temp.style
        styler = styler.map(highlight_patients, subset=["Дата последнего ТАПа в ЕМИАС"])
        styler.to_excel(metric_path + "\\" + str(department) + ".xlsx", index=False)


def highlight_patients(s):
    background = ""
    if not isinstance(s, pd._libs.tslibs.nattype.NaTType):
        if s <= (date.today() - timedelta(days=30)):
            background = "background-color: red"
        elif (s <= (date.today() - timedelta(days=20))) and (
            s >= (date.today() - timedelta(days=29))
        ):
            background = "background-color: yellow"
    return background


if __name__ == "__main__":
    PATH_TO_MYSQL_CREDENTIAL = (
        r"E:\System\Projects\pokb-metrics-collector\auth-mysql.json"
    )
    EXPORT_PATH = (
        r"E:\System\Projects\pokb-metrics-collector\reports\Показатель 187"
    )

    start_mysql_export()
