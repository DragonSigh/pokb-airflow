import pandas as pd
import gspread as gs

from datetime import date, timedelta

from oauth2client.service_account import ServiceAccountCredentials

# Настройки
PATH_TO_CREDENTIAL = "/home/user/pokb-399111-f04c71766977.json"
SPREADSHEET_KEY = "1cikHhnfVLZY7Jx6hOVHqDLD51897wwD4FIEw0_8zhc4"
UPLOAD_FILE_PATH = r"/etc/samba/share/upload/Отчет по вызовам в домене.xlsx"
SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
CREDENTIALS = ServiceAccountCredentials.from_json_keyfile_name(
    PATH_TO_CREDENTIAL, SCOPE
)


def upload_results():
    df_calls = pd.read_excel(UPLOAD_FILE_PATH, header=0)

    df_agg = df_calls.groupby("Статус").agg({"Тип": "count"})
    df_agg = df_agg.reset_index()
    df_agg.columns = ["Статус", "Количество"]

    df_agg2 = (
        df_calls[df_calls["Статус"] == "успешный"]
        .groupby("Первый ответивший")
        .agg({"Тип": "count"})
    )

    df_agg2 = df_agg2.reset_index()
    df_agg2.columns = ["Номер", "Количество ответов"]

    # Заливка в таблицу Google
    gc = gs.authorize(CREDENTIALS)
    spreadsheet = gc.open_by_key(SPREADSHEET_KEY)

    values = [df_agg.columns.values.tolist()]
    values.extend(df_agg.values.tolist())

    wks = "Сводная статистика!B4"
    worksheet = spreadsheet.worksheet("Сводная статистика")
    worksheet.batch_clear(["B9:C30"])

    spreadsheet.values_update(
        wks, params={"valueInputOption": "USER_ENTERED"}, body={"values": values}
    )

    values = [df_agg2.columns.values.tolist()]
    values.extend(df_agg2.values.tolist())

    wks = "Сводная статистика!B9"
    spreadsheet.values_update(
        wks, params={"valueInputOption": "USER_ENTERED"}, body={"values": values}
    )

    df_calls["Дата вызова"] = pd.to_datetime(
        df_calls["Дата вызова"], format="%Y-%m-%d"
    ).dt.date

    df_10days = df_calls[df_calls["Дата вызова"] >= (date.today() - timedelta(days=10))]

    df_10days = df_10days[df_10days["Статус"] == "успешный"]
    df_10days = df_10days[["Дата вызова", "Первый ответивший", "Время вызова"]]

    df_10days = (
        df_10days.groupby(["Первый ответивший", "Дата вызова"], observed=True)
        .count()
        .reset_index()
    )

    df_10days = df_10days.pivot_table(
        index="Первый ответивший",
        columns="Дата вызова",
        values="Время вызова",
        fill_value=0,
        aggfunc="sum",
    ).reset_index()

    df_10days["Итого"] = df_10days.sum(axis=1, skipna=True).astype(int)

    df_10days = df_10days.reset_index().rename_axis(None, axis=1)

    values = [df_10days.columns.values.tolist()]
    values.extend(df_10days.values.tolist())

    wks = "Все звонки за 10 дней"
    spreadsheet.values_update(
        wks, params={"valueInputOption": "USER_ENTERED"}, body={"values": values}
    )
