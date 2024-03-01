import metrics_collector.utils as utils
import pandas as pd
from sqlalchemy import create_engine
from datetime import date, datetime, timedelta
import json
import os
import gspread as gs
from oauth2client.service_account import ServiceAccountCredentials

# Настройки
FULL_PATH_TO_SCRIPT = os.path.abspath(__file__)
SCRIPT_DIRECTORY = str(os.path.dirname(FULL_PATH_TO_SCRIPT))
EXPORT_PATH = r"/etc/samba/share/download/Мониторинг диспансеирзации"
SQL_QUERY_FILE_PATH = SCRIPT_DIRECTORY + r"/metrics_collector/pokb_get_taps_disp.sql"
PATH_TO_MYSQL_CREDENTIAL = r"/home/user/auth-mysql.json"
PATH_TO_GSHEETS_CREDENTIAL = r"/home/user/pokb-399111-f04c71766977.json"
#SPREADSHEET_KEY = r"17U0jjvNCvrqLbu3MT5aiI4FCh4ChAV5B_7PT302aKhE"
SPREADSHEET_KEY = r"1NIzWfTgzLlIdTvHKy7syRB60IhP4Msndma6NONSFCT0"  # тест
SCOPE = [
    r"https://spreadsheets.google.com/feeds",
    r"https://www.googleapis.com/auth/drive",
]

# Вкладки
_CR_DISP = "Мониторинг диспансеризации"
_CR_LAST_WEEK_DIV = "За прошлую неделю (отделения)"
_CR_LAST_WEEK_DOC = "За прошлую неделю (врачи)"
_CR_ERRORS = "Проведено вне ОСП"

# Константы
_YEAR_PLAN = 184233 # Годовой план для Подольской ОКБ
_WORK_DAY_PLAN = _YEAR_PLAN // 248 # Количество рабочих дней в году

# Веса для распределения по подразделениям
_WEIGHTS = {
    "ОСП 1": 0.304,
    "ОСП 2": 0.065,
    "ОСП 3": 0.148,
    "ОСП 4": 0.139,
    "ОСП 5": 0.081,
    "ОСП 6": 0.095,
    "ОСП 7": 0.077,
    "Ленинградская 9": 0.091,
}


# Список праздников
holidays = [
    date(2024, 1, 1),  # Новый год
    date(2024, 1, 2),  # Новый год
    date(2024, 1, 3),  # Новый год
    date(2024, 1, 4),  # Новый год
    date(2024, 1, 5),  # Новый год
    date(2024, 1, 6),  # Новый год
    date(2024, 1, 8),  # Новый год
    date(2024, 1, 7),  # Рождество
    date(2024, 2, 23),  # День защитника Отечества
    date(2024, 3, 8),  # Международный женский день
    date(2024, 5, 1),  # Праздник Весны и Труда
    date(2024, 5, 9),  # День Победы
    date(2024, 6, 12),  # День России
    date(2024, 11, 4),  # День народного единства
]


def workdays_between(start_date, end_date, holidays):
    """
    Функция для вычисления количества рабочих дней между двумя датами включительно.

    Args:
      start_date: Дата начала (объект datetime).
      end_date: Дата конца (объект datetime).

    Returns:
      Количество рабочих дней (int).
    """

    if start_date > end_date:
        raise ValueError("Дата начала должна быть меньше или равна дате конца.")

    # Вычисляем общее количество дней
    total_days = (end_date - start_date).days + 1

    # Создаем список выходных дней
    weekends = [5, 6]  # суббота и воскресенье

    # Считаем количество выходных дней
    weekend_days = 0
    for day in range(total_days):
        date = start_date + timedelta(days=day)
        weekday = date.weekday()
        if weekday in weekends:
            weekend_days += 1
        # Проверка, является ли день праздником
        elif date in holidays:
            weekend_days += 1

    # Возвращаем количество рабочих дней
    return total_days - weekend_days


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
    df["date_close"] = pd.to_datetime(df["date_close"], format="%Y-%m-%d").dt.date

    # Выделяем общее ОСП
    df["subdivision_short"] = df["subdivision_name"].apply(utils.get_department)

    osp_list = [
        "Ленинградская 9",
        "ОСП 1",
        "ОСП 2",
        "ОСП 3",
        "ОСП 4",
        "ОСП 5",
        "ОСП 6",
        "ОСП 7",
    ]

    # Понедельник прошлой недели
    past_week_monday = (
        date.today() - timedelta(days=date.today().weekday()) - timedelta(days=7)
    )
    # Воскресенье прошлой недели
    past_week_sunday = past_week_monday + timedelta(days=6)

    # # Если нужны конкретные даты
    # past_week_monday = (
    #     date.today() - timedelta(days=date.today().weekday()) - timedelta(days=7)
    # )
    # past_week_sunday = date(datetime.now().year, 2, 29)

    first_date = past_week_monday.strftime("%d.%m")
    last_date = past_week_sunday.strftime("%d.%m")

    # Начало года
    year_beginning = date(datetime.now().year, 1, 1)

    # Количество рабочих дней с начала года
    current_workdays = workdays_between(year_beginning, past_week_sunday, holidays)

    print(current_workdays)

    # ОШИБКИ - проведено вне ОСП
    df_errors = df[~df["subdivision_short"].isin(osp_list)].fillna(0)

    df_errors["date_close"] = df_errors["date_close"].astype(str)

    df_errors = df_errors.rename(
        columns={
            "subdivision_short": "Отделение",
            "doctor_full_name": "ФИО",
            "card_number": "Номер карты",
            "date_close": "Дата закрытия",
        }
    )[["Отделение", "ФИО", "Номер карты", "Дата закрытия"]].sort_values(
        ["Отделение", "ФИО"]
    )

    print(df_errors)
    df = df[df["subdivision_short"].isin(osp_list)]

    # С НАЧАЛА ГОДА

    df_agg_year = df[df["date_close"] <= past_week_sunday]

    df_agg_year = (
        df_agg_year.groupby(["subdivision_short"])
        .agg(
            {
                "card_number": "count",
            }
        )
        .reset_index()
    )

    print(df_agg_year)

    df_agg_year["Годовой план"] = df_agg_year.apply(
        lambda row: int(_YEAR_PLAN * _WEIGHTS[row["subdivision_short"]]), axis=1
    )

    df_agg_year[f"Цель на {last_date}"] = df_agg_year.apply(
        lambda row: int(
            _WORK_DAY_PLAN * current_workdays * _WEIGHTS[row["subdivision_short"]]
        ),
        axis=1,
    )

    df_agg_year["Эффективность реализации годового плана, %"] = df_agg_year.apply(
        lambda row: round(
            100
            * row["card_number"]
            / (_WORK_DAY_PLAN * current_workdays * _WEIGHTS[row["subdivision_short"]]),
            2,
        ),
        axis=1,
    )

    df_agg_year = df_agg_year.rename(
        columns={
            "subdivision_short": "ОСП",
            "card_number": f"Проведено на {last_date}",
        }
    )[
        [
            "ОСП",
            "Годовой план",
            f"Цель на {last_date}",
            f"Проведено на {last_date}",
            "Эффективность реализации годового плана, %",
        ]
    ]

    df_agg_year.loc["ПОКБ"] = df_agg_year.sum(numeric_only=True)
    df_agg_year.loc["ПОКБ", ["ОСП"]] = "ПОКБ"
    df_agg_year.loc["ПОКБ", ["Эффективность реализации годового плана, %"]] = round(
        100
        * df_agg_year.at["ПОКБ", f"Проведено на {last_date}"]
        / df_agg_year.at["ПОКБ", f"Цель на {last_date}"],
        2,
    )

    # ЗА НЕДЕЛЮ

    week_column_format = f"за неделю (с {first_date} по {last_date})"

    df_week = df[
        (df["date_close"] >= past_week_monday) & (df["date_close"] <= past_week_sunday)
    ]

    df_agg_past_week = (
        df_week.groupby(["subdivision_short"])
        .agg(
            {
                "card_number": "count",
            }
        )
        .reset_index()
    )

    # ЗА НЕДЕЛЮ - ОТДЕЛЕНИЯ

    # С начала года нарастающим итогом - отделения
    df_agg_past_week_div = (
        df[df["date_close"] <= past_week_sunday]
        .groupby(["subdivision_name"])
        .agg(
            {
                "card_number": "count",
            }
        )
        .reset_index()
        .sort_values("subdivision_name")
        .rename(
            columns={
                "subdivision_name": "Отделение",
                "card_number": f"Проведено с начала года",
            }
        )
    )

    df_agg_past_week_div = df_agg_past_week_div.merge(
        df_week.groupby(["subdivision_name"])
        .agg(
            {
                "card_number": "count",
            }
        )
        .reset_index()
        .sort_values("subdivision_name")
        .rename(
            columns={
                "subdivision_name": "Отделение",
                "card_number": f"Проведено {week_column_format}",
            }
        ),
        on="Отделение",
        how="left",
    ).fillna(0)

    # ЗА НЕДЕЛЮ - ВРАЧИ

    # С начала года нарастающим итогом - врачи
    df_agg_past_week_doc = (
        df[df["date_close"] <= past_week_sunday]
        .groupby(["subdivision_name", "doctor_full_name"])
        .agg(
            {
                "card_number": "count",
            }
        )
        .reset_index()
        .sort_values(["subdivision_name", "doctor_full_name"])
        .rename(
            columns={
                "subdivision_name": "Отделение",
                "doctor_full_name": "ФИО",
                "card_number": f"Проведено с начала года",
            }
        )
    )

    df_agg_past_week_doc = df_agg_past_week_doc.merge(
        df_week.groupby(["subdivision_name", "doctor_full_name"])
        .agg(
            {
                "card_number": "count",
            }
        )
        .reset_index()
        .sort_values(["subdivision_name", "doctor_full_name"])
        .rename(
            columns={
                "subdivision_name": "Отделение",
                "doctor_full_name": "ФИО",
                "card_number": f"Проведено {week_column_format}",
            }
        ),
        on=["Отделение", "ФИО"],
        how="left",
    ).fillna(0)

    # Количество рабочих дней с начала года
    current_workdays_in_week = workdays_between(past_week_monday, past_week_sunday, holidays)

    df_agg_past_week[f"Цель {week_column_format}"] = df_agg_past_week.apply(
        lambda row: int(_WORK_DAY_PLAN * current_workdays_in_week * _WEIGHTS[row["subdivision_short"]]),
        axis=1,
    )

    df_agg_past_week[f"Эффективность {week_column_format}, %"] = df_agg_past_week.apply(
        lambda row: round(
            100
            * row["card_number"]
            / (_WORK_DAY_PLAN * current_workdays_in_week * _WEIGHTS[row["subdivision_short"]]),
            2,
        ),
        axis=1,
    )

    df_agg_past_week = df_agg_past_week.rename(
        columns={
            "subdivision_short": "ОСП",
            "card_number": f"Проведено {week_column_format}",
        }
    )[
        [
            "ОСП",
            f"Цель {week_column_format}",
            f"Проведено {week_column_format}",
            f"Эффективность {week_column_format}, %",
        ]
    ]

    df_agg_past_week.loc["ПОКБ"] = df_agg_past_week.sum(numeric_only=True)
    df_agg_past_week.loc["ПОКБ", ["ОСП"]] = "ПОКБ"
    df_agg_past_week.loc["ПОКБ", [f"Эффективность {week_column_format}, %"]] = round(
        100
        * df_agg_past_week.at["ПОКБ", f"Проведено {week_column_format}"]
        / df_agg_past_week.at["ПОКБ", f"Цель {week_column_format}"],
        2,
    )

    print(df_agg_year)
    print(df_agg_past_week)

    CREDENTIALS = ServiceAccountCredentials.from_json_keyfile_name(
        PATH_TO_GSHEETS_CREDENTIAL, SCOPE
    )

    df_final = df_agg_year.merge(df_agg_past_week, how="left", on="ОСП")

    df_final = df_final.fillna("")

    # Заливка в таблицу Google
    gc = gs.authorize(CREDENTIALS)
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)

    wks = _CR_DISP + "!A1"
    worksheet = spreadsheet.worksheet(_CR_DISP)

    df_current = pd.DataFrame(worksheet.get_all_values())

    # Выбрать первую строку в качестве столбцов
    df_current.columns = df_current.iloc[0]
    df_current = df_current[1:]  # Удалить первую строку
    df_current = df_current.reset_index(drop=True)  # Сброс индексов
    # Удалить колонки с 1 по 4 включительно
    df_current = df_current.drop(df_current.columns[1:5], axis=1)
    print(df_current)

    # Если Google таблица не пустая, добавляем исторические данные
    if not df_current.empty:
        df_final = df_final.merge(df_current, how="left", on="ОСП")

    values = [df_final.columns.values.tolist()]
    values.extend(df_final.values.tolist())

    worksheet.batch_clear(["A1:Z500"])

    spreadsheet.values_update(
        wks, params={"valueInputOption": "USER_ENTERED"}, body={"values": values}
    )

    # Отделения
    wks = _CR_LAST_WEEK_DIV + "!A1"
    worksheet = spreadsheet.worksheet(_CR_LAST_WEEK_DIV)
    values = [df_agg_past_week_div.columns.values.tolist()]
    values.extend(df_agg_past_week_div.values.tolist())
    worksheet.batch_clear(["A1:Z500"])
    spreadsheet.values_update(
        wks, params={"valueInputOption": "USER_ENTERED"}, body={"values": values}
    )

    # Врачи
    wks = _CR_LAST_WEEK_DOC + "!A1"
    worksheet = spreadsheet.worksheet(_CR_LAST_WEEK_DOC)
    values = [df_agg_past_week_doc.columns.values.tolist()]
    values.extend(df_agg_past_week_doc.values.tolist())
    worksheet.batch_clear(["A1:Z500"])
    spreadsheet.values_update(
        wks, params={"valueInputOption": "USER_ENTERED"}, body={"values": values}
    )

    # Ошибки вне ОСП
    wks = _CR_ERRORS + "!A1"
    worksheet = spreadsheet.worksheet(_CR_ERRORS)
    values = [df_errors.columns.values.tolist()]
    values.extend(df_errors.values.tolist())
    worksheet.batch_clear(["A1:Z500"])
    spreadsheet.values_update(
        wks, params={"valueInputOption": "USER_ENTERED"}, body={"values": values}
    )


if __name__ == "__main__":
    PATH_TO_MYSQL_CREDENTIAL = (
        r"E:\System\Projects\pokb-metrics-collector\auth-mysql.json"
    )
    PATH_TO_GSHEETS_CREDENTIAL = (
        r"E:\System\Projects\pokb-metrics-collector\pokb-399111-f04c71766977.json"
    )
    start_mysql_export()


# 1. Грузим раз в неделю в 15:00 прошлую неделю две цифры: абсолютное значение в количестве прошедших
# диспансеризацию и процент выполнения плана за эту неделю (не нарастающим, а именно за неделю) - результат
# грузим в гугл-таблицу и ячейку с % выполнения красим цветом по следующему принципу: диапазон баллов: отставание
# от плана менее 1,97% - 5 баллов (зел); 1,97 – 2,99 – 3 балла (жел); 3 и более – 0 баллов (красн).
# 2. Вторым столбцом после названия ОСП должен стоять годовой план; следующий столбец абсолютный план по состоянию
# на конец периода из п.1; следующий столбец - абсолютный факт нарастающим с 1 января до конца периода из п.1;
# следующий столбец - % выполнения по аналогии с п.1
# 3. Каждая загруженная неделя должна фиксироваться и каждый раз не перевыгружаться, то есть ранее загруженные данные
# должны сохраняться, и даже если они задним числом что-то довнесут, то цифра не должна в таблице поменять - эти изменения
# будут учтены только в нарастающем с начала года.

# P.S. кажется, что лучше, чтобы недели шли слева направо по убыванию, то есть сначала последняя загруженная неделя,
# далее предыдущая, а последняя - начало года - но эта хотелка моя, ее можно реализовать потом, когда будет время, если она проблемная
