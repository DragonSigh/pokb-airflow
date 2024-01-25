import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from datetime import date, timedelta
import json
import os

import gspread as gs
from oauth2client.service_account import ServiceAccountCredentials

# Настройки
FULL_PATH_TO_SCRIPT = os.path.abspath(__file__)
SCRIPT_DIRECTORY = str(os.path.dirname(FULL_PATH_TO_SCRIPT))
EXPORT_PATH = r"/etc/samba/share/download/Расписание"
SQL_QUERY_FILE_PATH = SCRIPT_DIRECTORY + r"/metrics_collector/pokb_get_schedule.sql"
PATH_TO_MYSQL_CREDENTIAL = r"/home/user/auth-mysql.json"
PATH_TO_GSHEETS_CREDENTIAL = r"/home/user/pokb-399111-f04c71766977.json"
SPREADSHEET_KEY = r"1gIn_J_m_hkkU7KHMYWXulOHF8wJ3fRLHLt4qEnKai3s"
SCOPE = [
    r"https://spreadsheets.google.com/feeds",
    r"https://www.googleapis.com/auth/drive",
]
CREDENTIALS = ServiceAccountCredentials.from_json_keyfile_name(
    PATH_TO_GSHEETS_CREDENTIAL, SCOPE
)


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

    # print(df.info())
    # print(df.head(5))

    # КР 26, 46, 61, 72
    # Расписание создано на 3 недели вперед
    # 100% (необходимо проставить выходной, больничный отпуск, работа в др подразделений, чтоб не было пустот)

    missed_days = []
    nearest_cells = []
    nearest_cells_spec = []
    nearest_cells_eq = []
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
            if df_temp["resource_type"].iloc[0] == "Врач":
                resource_name = df_temp["doctor_full_name"].iloc[0]
            elif df_temp["resource_type"].iloc[0] == "Оборудование":
                resource_name = df_temp["equipment_name"].iloc[0]
            elif df_temp["resource_type"].iloc[0] == "Кабинет":
                resource_name = df_temp["cabinet_number"].iloc[0]
            row = [
                df_temp["resource_type"].iloc[0],
                df_temp["subdivision_name"].iloc[0],
                df_temp["department_name"].iloc[0],
                resource_name,
                missed_dates,
            ]
            missed_days.append(row)
        # Суммарное время
        today_timestamp = pd.Timestamp("today")
        tomorrow_timestamp = pd.Timestamp("today").normalize() + pd.Timedelta(days=1)
        df_sum = df_temp[
            (df_temp["access_code"] != 111)
            & (df_temp["begin_time"] > today_timestamp.normalize())
            & (df_temp["begin_time"] < tomorrow_timestamp
            & (df_temp["resource_type"] == "Врач")) # ТОЛЬКО ВРАЧИ НА 3 недели
        ]
        # TODO
        df_sum["diff_minutes"] = df_sum["end_time"] - df_sum["begin_time"]
        df_sum["diff_minutes"] = df_sum["diff_minutes"] / pd.Timedelta(minutes=1)
        # print(df_sum["diff_minutes"])
        # Доступность оборудования
        df_temp_eq = df_temp[
            (df_temp["is_used"] == 0)
            & (
                (df_temp["end_time"] > pd.Timestamp("today"))
                & (df_temp["resource_type"] == "Оборудование")
            )
        ]
        if not df_temp_eq.empty:
            # МРТ и КТ
            mri_ct_ids = [
                "Магнитно-резонансный томограф",
                "Компьютерный томограф",
                "Компьютерные томографы",
            ]
            if df_temp_eq["equipment_type"].iloc[0] in mri_ct_ids:
                # Количество дней ожидания до свободной ячейки для врача
                if df_temp_eq[df_temp_eq["ac_doctor"] == 1].empty:
                    nearest_day = 9999  # не найдено
                else:
                    nearest_day = (
                        df_temp_eq[df_temp_eq["ac_doctor"] == 1]["end_time"]
                        .iloc[0]
                        .normalize()
                        - pd.Timestamp("today").normalize()
                    ).days
                row = [
                    df_temp_eq["subdivision_name"].iloc[0],
                    df_temp_eq["department_name"].iloc[0],
                    df_temp_eq["equipment_name"].iloc[0],
                    df_temp_eq["specialty_name"].iloc[0],
                    nearest_day,
                ]
                nearest_cells_eq.append(row)

        # Доступность врачей
        df_temp = df_temp[
            (df_temp["is_used"] == 0)
            & (
                (df_temp["end_time"] > pd.Timestamp("today"))
                & (df_temp["resource_type"] == "Врач")
            )
        ]
        if not df_temp.empty:
            # Терапевты, участковые терапевты, педиатры, участковые педиатры, ВОП
            ter_ped_vop_ids = [1815, 1816, 1857, 1858, 1895]
            if df_temp["specialty_id"].iloc[0] in ter_ped_vop_ids:
                # Количество дней ожидания до свободной ячейки для врача
                if df_temp[df_temp["ac_doctor"] == 1].empty:
                    nearest_day = 9999  # не найдено
                else:
                    nearest_day = (
                        df_temp[df_temp["ac_doctor"] == 1]["end_time"]
                        .iloc[0]
                        .normalize()
                        - pd.Timestamp("today").normalize()
                    ).days
                # Количество дней ожидания до свободной ячейки для самозаписи
                if df_temp[df_temp["ac_internet"] == 1].empty:
                    nearest_day_internet = 9999  # не найдено
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
            else:
                # Узкие специалисты
                # Количество дней ожидания до свободной ячейки для врача
                if df_temp[df_temp["ac_doctor"] == 1].empty:
                    nearest_day = 9999  # не найдено
                else:
                    nearest_day = (
                        df_temp[df_temp["ac_doctor"] == 1]["end_time"]
                        .iloc[0]
                        .normalize()
                        - pd.Timestamp("today").normalize()
                    ).days
                # Количество дней ожидания до свободной ячейки для самозаписи
                if df_temp[df_temp["ac_internet"] == 1].empty:
                    nearest_day_internet = 9999  # не найдено
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
                nearest_cells_spec.append(row)
        # else:
        # Врач без свободных ячеек
        # print(df[df["resource_id"] == resource]["doctor_full_name"].iloc[0])

    # Создать папку для выгрузки метрики
    try:
        os.mkdir(EXPORT_PATH)
    except FileExistsError:
        pass

    # Расписание создано на 3 недели вперед
    df_missed_days = pd.DataFrame(
        missed_days,
        columns=[
            "Тип ресурса",
            "Подразделение",
            "Отделение",
            "Название ресурса",
            "Даты без расписания",
        ],
    )

    df_missed_days["Даты без расписания"] = df_missed_days["Даты без расписания"].apply(
        lambda x: ", ".join(x)
    )

    df_missed_days = df_missed_days.sort_values("Подразделение")
    df_missed_days.to_excel(
        EXPORT_PATH + "/Расписание создано на 3 недели вперед.xlsx", index=False
    )

    # Права на скачивание любому пользователю
    os.chmod(EXPORT_PATH + "/Расписание создано на 3 недели вперед.xlsx", 0o777)

    # Заливка в таблицу Google
    gc = gs.authorize(CREDENTIALS)
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)

    # ДОСТУПНОСТЬ ТЕРАПЕВТОВ И ВОП
    values = [df_missed_days.columns.values.tolist()]
    values.extend(df_missed_days.values.tolist())

    wks = "Расписание на 3 недели!A1"
    worksheet = spreadsheet.worksheet("Расписание на 3 недели")
    worksheet.batch_clear(["A1:Z500"])
    spreadsheet.values_update(
        wks, params={"valueInputOption": "USER_ENTERED"}, body={"values": values}
    )

    df_nearest_cells = pd.DataFrame(
        nearest_cells,
        columns=[
            "Подразделение",
            "Отделение",
            "ФИО врача",
            "Специальность",
            "До ближайшей свободной ячейки (дней)",
            "До ячейки самозаписи (дней) (9999 = отсутствует)",
        ],
    )
    df_nearest_cells = df_nearest_cells[
        (df_nearest_cells["До ближайшей свободной ячейки (дней)"] > 1)
        | (df_nearest_cells["До ячейки самозаписи (дней) (9999 = отсутствует)"] > 1)
    ]
    df_nearest_cells = df_nearest_cells.sort_values("Подразделение")

    df_nearest_cells.to_excel(
        EXPORT_PATH + "/Ближайшие свободные ячейки тер воп.xlsx", index=False
    )

    # Права на скачивание любому пользователю
    os.chmod(EXPORT_PATH + "/Ближайшие свободные ячейки тер воп.xlsx", 0o777)

    values = [df_nearest_cells.columns.values.tolist()]
    values.extend(df_nearest_cells.values.tolist())

    wks = "Доступность терапевтов и ВОП!A1"
    worksheet = spreadsheet.worksheet("Доступность терапевтов и ВОП")
    worksheet.batch_clear(["A1:Z500"])
    spreadsheet.values_update(
        wks, params={"valueInputOption": "USER_ENTERED"}, body={"values": values}
    )

    # ДОСТУПНОСТЬ УЗКИХ СПЕЦИАЛИСТОВ

    df_nearest_cells_spec = pd.DataFrame(
        nearest_cells_spec,
        columns=[
            "Подразделение",
            "Отделение",
            "ФИО врача",
            "Специальность",
            "До ближайшей свободной ячейки (дней)",
            "До ячейки самозаписи (дней) (9999 = отсутствует)",
        ],
    )
    df_nearest_cells_spec = df_nearest_cells_spec[
        (df_nearest_cells_spec["До ближайшей свободной ячейки (дней)"] > 10)
        | (
            df_nearest_cells_spec["До ячейки самозаписи (дней) (9999 = отсутствует)"]
            > 10
        )
    ]
    df_nearest_cells_spec = df_nearest_cells_spec.sort_values("Подразделение")

    df_nearest_cells_spec.to_excel(
        EXPORT_PATH + "/Ближайшие свободные ячейки спец.xlsx", index=False
    )

    # Права на скачивание любому пользователю
    os.chmod(EXPORT_PATH + "/Ближайшие свободные ячейки спец.xlsx", 0o777)

    values = [df_nearest_cells_spec.columns.values.tolist()]
    values.extend(df_nearest_cells_spec.values.tolist())

    wks = "Доступность узких специалистов!A1"
    worksheet = spreadsheet.worksheet("Доступность узких специалистов")
    worksheet.batch_clear(["A1:Z500"])
    spreadsheet.values_update(
        wks, params={"valueInputOption": "USER_ENTERED"}, body={"values": values}
    )

    # ДОСТУПНОСТЬ МРТ И КТ

    # ДОСТУПНОСТЬ УЗКИХ СПЕЦИАЛИСТОВ

    df_nearest_cells_eq = pd.DataFrame(
        nearest_cells_eq,
        columns=[
            "Подразделение",
            "Отделение",
            "ФИО врача",
            "Специальность",
            "До ближайшей свободной ячейки (дней) (9999 = отсутствует)",
        ],
    )
    df_nearest_cells_eq = df_nearest_cells_eq[
        (df_nearest_cells_eq["До ближайшей свободной ячейки (дней) (9999 = отсутствует)"] > 9)
    ]
    df_nearest_cells_eq = df_nearest_cells_eq.sort_values("Подразделение")

    df_nearest_cells_eq.to_excel(
        EXPORT_PATH + "/Ближайшие свободные ячейки мрт и кт.xlsx", index=False
    )

    # Права на скачивание любому пользователю
    os.chmod(EXPORT_PATH + "/Ближайшие свободные ячейки мрт и кт.xlsx", 0o777)

    values = [df_nearest_cells_eq.columns.values.tolist()]
    values.extend(df_nearest_cells_eq.values.tolist())

    wks = "Доступность МРТ и КТ!A1"
    worksheet = spreadsheet.worksheet("Доступность МРТ и КТ")
    worksheet.batch_clear(["A1:Z500"])
    spreadsheet.values_update(
        wks, params={"valueInputOption": "USER_ENTERED"}, body={"values": values}
    )

    # df_dep = pd.DataFrame(dep_data, columns=["Отделение", "ФИО врача", "Даты без расписания"])
    # print(df_dep)
    # df_dep.to_excel(dep + ".xlsx", index=False)

    # КР 25, 45
    # Доступность педиатров и врачей специалистов
    # Количество дней до ближайшей свободной ячейки для записи/само записи, в днях.
    # Врачи-терапевты участковые, ВОП - 1 день, Узкие - 10 дней.
    # Педиатрия - 1 день, Узкие - 10 дней.
    # Учитывается среднее значение доступности за отчетный период в разрезе всех подразделений.

    # В выгрузку расписаний добавить (по возможности) число ставок, на которое оформлен врач
    # + считать суммарное время (сумма всех ячеек в день/период), на которое открыто расписание по ресурсу (перерывы должны вычитаться)
    # + считать доступность
    # разница в днях до первой свободной ячейки по:
    # - самозаписи (отдельно)
    # - для врача (отдельно)
    # не учитывать свободные талоны на сегодня на прошедшее время\
