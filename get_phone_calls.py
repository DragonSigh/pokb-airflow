import pandas as pd
import gspread as gs
import os

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
    spreadsheet.values_update(
        wks, params={"valueInputOption": "USER_ENTERED"}, body={"values": values}
    )

    values = [df_agg2.columns.values.tolist()]
    values.extend(df_agg2.values.tolist())

    wks = "Сводная статистика!B9"
    spreadsheet.values_update(
        wks, params={"valueInputOption": "USER_ENTERED"}, body={"values": values}
    )
