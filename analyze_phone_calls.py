import pandas as pd
import gspread as gs

from datetime import date, timedelta

from oauth2client.service_account import ServiceAccountCredentials

# Настройки
PATH_TO_CREDENTIAL = "/home/user/pokb-399111-f04c71766977.json"
SPREADSHEET_KEY = "1cikHhnfVLZY7Jx6hOVHqDLD51897wwD4FIEw0_8zhc4"
UPLOAD_FILE_PATH = r"/etc/samba/share/upload/Отчет по вызовам в домене.xlsx"
EXPORTH_PATH = r"/etc/samba/share/download/"
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


def get_name(x):
    """
    Дополняем телефон именами
    """
    value = str(x)
    if "tel701" in value:
        return "Сафронов (701)"
    elif "tel702" in value:
        return "Зенкин (702)"
    elif "tel703" in value:
        return "Черников (703)"
    elif "tel704" in value:
        return "Ермаков (704)"
    elif "tel705" in value:
        return "Александров (705)"
    elif "tel706" in value:
        return "Хожиматов (706)"
    elif "tel707" in value:
        return "Орехов (707)"
    elif "tel708" in value:
        return "Ларина (708)"
    elif "tel709" in value:
        return "Терентьев (709)"
    elif "tel710" in value:
        return "Седова (710)"
    elif "tel711" in value:
        return "Анашкин (711)"
    elif "tel712" in value:
        return "Жаров (712)"
    elif "tel713" in value:
        return "Лазанко (713)"
    elif "tel714" in value:
        return "Ковнеров (714)"
    elif "tel715" in value:
        return "Елистратов (715)"


def analyze_results():
    df_calls = pd.read_excel(UPLOAD_FILE_PATH, header=0)

    df_calls = df_calls[df_calls["Входящая линия"] == 74967534223]

    df_calls["Первый ответивший"] = df_calls["Первый ответивший"].apply(get_name)

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

    date_filter = date.today() - timedelta(days=10)

    df_10days = df_calls[df_calls["Дата вызова"] >= date_filter]
    df_10days["Дата вызова"] = df_10days["Дата вызова"].astype(str)

    suc_calls = df_10days[df_10days["Статус"] == "успешный"].shape[0]
    neg_calls = df_10days[df_10days["Статус"] == "пропущенный"].shape[0]

    # Всего отвечено
    df_10days_answered = df_10days[df_10days["Статус"] == "успешный"]
    df_10days_answered = df_10days_answered[
        ["Дата вызова", "Первый ответивший", "Время вызова"]
    ]

    df_10days_answered = (
        df_10days_answered.groupby(["Первый ответивший", "Дата вызова"], observed=True)
        .count()
        .reset_index()
    )

    df_10days_answered = df_10days_answered.pivot_table(
        index="Первый ответивший",
        columns="Дата вызова",
        values="Время вызова",
        fill_value=0,
        aggfunc="sum",
    )

    df_10days_answered.loc["Всего отвечено"] = df_10days_answered.sum(
        numeric_only=True, axis=0
    )

    # Всего пропущено
    df_10days_missed = (
        df_10days[(df_10days["Статус"] == "пропущенный")]
        .groupby("Дата вызова", observed=True)
        .count()
        .reset_index()
    )

    df_10days_missed = df_10days_missed[["Дата вызова", "Время вызова"]]
    df_10days_missed.columns = ["Дата вызова", "Всего пропущено"]

    # Заполняем даты без пропущенных звонков нулями
    dtr = pd.date_range(date_filter, date.today(), freq="D")
    s = pd.Series(dtr)
    df_10days_missed = pd.concat(
        [df_10days_missed, s[~s.index.isin(df_10days_missed.index)]]
    )
    df_10days_missed = df_10days_missed.drop([0], axis=1).fillna(0)

    df_10days_missed = df_10days_missed.pivot_table(
        columns="Дата вызова",
        values="Всего пропущено",
        fill_value=0,
        aggfunc="sum",
    ).reset_index()

    df_10days_answered = df_10days_answered.reset_index().rename_axis(None, axis=1)

    values = [df_10days_answered.columns.values.tolist()]
    values.extend(df_10days_answered.values.tolist())
    values.extend(df_10days_missed.values.tolist())

    wks = "Все звонки за 10 дней"
    worksheet = spreadsheet.worksheet(wks)
    worksheet.batch_clear(["A1:Q30"])

    spreadsheet.values_update(
        wks, params={"valueInputOption": "USER_ENTERED"}, body={"values": values}
    )

    worksheet.update_acell("A28", "Отвечено за 10 дней")
    worksheet.update_acell("B28", suc_calls)
    worksheet.update_acell("A29", "Пропущено за 10 дней")
    worksheet.update_acell("B29", neg_calls)
