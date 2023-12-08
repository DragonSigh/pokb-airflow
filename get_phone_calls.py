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

def next_available_row(sheet, cols_to_sample=2):
    # looks for empty row based on values appearing in 1st N columns
    cols = sheet.range(1, 1, sheet.row_count, cols_to_sample)
    return max([cell.row for cell in cols if cell.value]) + 1


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
    df_10days["Дата вызова"] = df_10days["Дата вызова"].astype(str)

    suc_calls = df_10days[df_10days["Статус"] == "успешный"].shape[0]
    neg_calls =  df_10days[df_10days["Статус"] == "пропущенный"].shape[0]

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
    )

    df_10days["Итого"] = df_10days.sum(axis=1, skipna=True).astype(int)

    df_10days = df_10days.reset_index().rename_axis(None, axis=1)

    values = [df_10days.columns.values.tolist()]
    values.extend(df_10days.values.tolist())

    wks = "Все звонки за 10 дней"
    worksheet = spreadsheet.worksheet(wks)
    worksheet.batch_clear(["A1:Q30"])
    
    spreadsheet.values_update(
        wks, params={"valueInputOption": "USER_ENTERED"}, body={"values": values}
    )

    next_row = next_available_row(worksheet)
    worksheet.update_acell("A{}".format(next_row), "Отвечено за 10 дней")
    worksheet.update_acell("B{}".format(next_row), suc_calls)
    worksheet.update_acell("A{}".format(next_row), "Пропущено за 10 дней")
    worksheet.update_acell("B{}".format(next_row), neg_calls)