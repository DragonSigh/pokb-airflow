import pandas as pd
from sqlalchemy import create_engine
from datetime import date, timedelta
import json
import os


def start_mysql_export():
    FULL_PATH_TO_SCRIPT = os.path.abspath(__file__)
    SCRIPT_DIRECTORY = str(os.path.dirname(FULL_PATH_TO_SCRIPT))
    SQL_QUERY_FILE_PATH = SCRIPT_DIRECTORY + r"/metrics_collector/pokb_get_schedule.sql"
    PATH_TO_CREDENTIAL = r"/home/user/auth-mysql.json"
    EXPORT_PATH = r"/etc/samba/share/download/Расписание"

    with open(SQL_QUERY_FILE_PATH, "r", encoding="utf-8") as file:
        sql_query = file.read()

    with open(PATH_TO_CREDENTIAL) as f:
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

    # print(df.info())
    # print(df.head(5))

    # КР 26, 46, 61, 72
    # Расписание создано на 3 недели вперед
    # 100% (необходимо проставить выходной, больничный отпуск, работа в др подразделений, чтоб не было пустот)

    missed_days = []
    nearest_cells = []
    for resource in df["resource_id"].unique():
        df_temp = df[df["resource_id"] == resource]
        # Расписание создано на 3 недели вперед
        missed_dates = (
            pd.date_range(start=date.today(), end=(date.today() + timedelta(days=21)))
            .difference(df_temp["begin_time"].dt.date)
            .strftime("%Y-%m-%d")
            .tolist()
        )
        if missed_dates:
            row = [
                df_temp["subdivision_name"].iloc[0],
                df_temp["department_name"].iloc[0],
                df_temp["doctor_full_name"].iloc[0],
                missed_dates,
            ]
            missed_days.append(row)
        # Суммарное время
        today_timestamp = pd.Timestamp("today")
        tomorrow_timestamp = pd.Timestamp("today").normalize() + pd.Timedelta(days=1)
        df_sum = df_temp[
            (df_temp["access_code"] != 111)
            & (df_temp["begin_time"] > today_timestamp.normalize())
            & (df_temp["begin_time"] < tomorrow_timestamp)
        ]
        # TODO
        df_sum["diff_minutes"] = df_sum["end_time"] - df_sum["begin_time"]
        df_sum["diff_minutes"] = df_sum["diff_minutes"] / pd.Timedelta(minutes=1)
        # print(df_sum["diff_minutes"])
        # Доступность педиатров и врачей специалистов
        df_temp = df_temp[
            (df_temp["is_used"] == 0) & (df_temp["end_time"] > pd.Timestamp("today"))
        ]
        if not df_temp.empty:
            # Терапевты, участковые терапевты, педиатры, участковые педиатры, ВОП
            ter_ped_vop_ids = [1815, 1816, 1857, 1858, 1895]
            if df_temp["specialty_id"].iloc[0] in ter_ped_vop_ids:
                # Количество дней ожидания до свободной ячейки для врача
                nearest_day = (
                    df_temp[df_temp["ac_doctor"] == 1]["end_time"].iloc[0].normalize()
                    - pd.Timestamp("today").normalize()
                ).days
                # Количество дней ожидания до свободной ячейки для самозаписи
                if df_temp[df_temp["ac_internet"] == 1].empty:
                    nearest_day_internet = -9999  # не найдено
                else:
                    nearest_day_internet = (
                        df_temp[df_temp["ac_internet"] == 1]["end_time"]
                        .iloc[0]
                        .normalize()
                        - pd.Timestamp("today").normalize()
                    ).days
                row = [
                    df_temp["subdivision_name"].iloc[0],
                    df_temp["department_name"].iloc[0],
                    df_temp["doctor_full_name"].iloc[0],
                    df_temp["specialty_name"].iloc[0],
                    nearest_day,
                    nearest_day_internet,
                ]
                nearest_cells.append(row)
        # else:
        # Врач без свободных ячеек
        # print(df[df["resource_id"] == resource]["doctor_full_name"].iloc[0])

    # Создать папку для выгрузки метрики
    try:
        os.mkdir(EXPORT_PATH)
    except FileExistsError:
        pass

    df_missed_days = pd.DataFrame(missed_days, columns=["Подразделение", "Отделение", "ФИО врача", "Даты без расписания"])
    df_missed_days.to_excel(EXPORT_PATH + "/Расписание создано на 3 недели вперед.xslx", index=False)

    # Права на скачивание любому пользователю
    os.chmod(EXPORT_PATH + "/Расписание создано на 3 недели вперед.xslx", 0o777)

    # df_dep = pd.DataFrame(dep_data, columns=["Отделение", "ФИО врача", "Даты без расписания"])
    # print(df_dep)
    # df_dep.to_excel(dep + ".xlsx", index=False)




    # КР 25, 45
    # Доступность педиатров и врачей специалистов
    # Количество дней до ближайшей свободной ячейки для записи/само записи, в днях.
    # Врачи-терапевты участковые, ВОП - 1 день, Узкие - 10 дней.
    # Педиатрия - 1 день, Узкие - 10 дней.
    # Учитывается среднее значение доступности за отчетный период в разрезе всех подразделений.

    # print(dp_ter_ped.head())

    # В выгрузку расписаний добавить (по возможности) число ставок, на которое оформлен врач
    # + считать суммарное время (сумма всех ячеек в день/период), на которое открыто расписание по ресурсу (перерывы должны вычитаться)
    # + считать доступность
    # разница в днях до первой свободной ячейки по:
    # - самозаписи (отдельно)
    # - для врача (отдельно)
    # не учитывать свободные талоны на сегодня на прошедшее время\
