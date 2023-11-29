import json
import pandas as pd
import re
import os


UPLOAD_FILE_PATH = r"/etc/samba/share/upload/result.json"
DOWNLOAD_FILE_PATH = r"/etc/samba/share/download/Статусы заявок ТП ПОКБ в телеграм.xlsx"


def get_groups(x):
    """
    Выделяем из хештегов группы, на которые оформлена заявка
    """
    value = str(x)
    result = []

    if re.search(r"#ОСП.*\d", value):
        result = re.findall(r"#(ОСП_\d)", value)
    if re.search(r"#Кирова.*38", value):
        result.append("Кирова_38")
    if re.search(r"#Ленинград", value):
        result.append("Ленинградская_9")
    return result


def get_themes(x):
    """
    Выделяем из хештегов тематику заявки
    """
    value = str(x)
    result = []

    if re.search(r"#ЕМИАС", value):
        result.append("ЕМИАС")
    if re.search(r"#Оснащение", value):
        result.append("Оснащение")
    if re.search(r"#Списание", value):
        result.append("Списание")
    if re.search(r"#Проект", value):
        result.append("Проект")
    return result


def get_status(x):
    """
    Выделяем статус решена или нет заявка
    """
    value = str(x)

    if re.search(r"#Решено", value):
        return "Решено"
    else:
        return "Открыто"


def get_clean_data(x):
    chars_list = ["'", "[", "]"]
    return str(x).translate({ord(x): "" for x in chars_list})


def analyze_results():
    # Открыть и загрузить json
    f = open(UPLOAD_FILE_PATH, encoding="utf8")
    data = json.load(f)

    # Выдернуть только сообщения из json
    msgs = data["messages"]
    dmain = pd.DataFrame(msgs)

    # Создать новый датафрейм только с требуемыми колонками (id, author и text)
    df_new = dmain.filter(items=["id", "author", "text"])

    # Предварительная обработка данных

    # Пропустить первые вводные строки
    df_new = df_new[3:]
    # Дропнуть технические записи без автора
    df_new = df_new.dropna(subset="author")

    # Преобразовать в текст
    df_new["text"] = df_new["text"].astype(str)

    # Замена разночтений
    df_new["text"] = df_new["text"].str.replace("Заявка № ", "Заявка №")
    df_new["text"] = df_new["text"].str.replace("Заявка  №", "Заявка №")
    df_new["text"] = df_new["text"].str.replace("Заявка N ", "Заявка №")
    df_new["text"] = df_new["text"].str.replace("Заявка N", "Заявка №")
    df_new["text"] = df_new["text"].str.replace("#Открыта", "#Открыто")
    df_new["text"] = df_new["text"].str.replace("#Решена", "#Решено")
    df_new["text"] = df_new["text"].str.replace("#решено", "#Решено")
    df_new["text"] = df_new["text"].str.replace("#Закрыта", "#Решено")

    # Извлечь номер заявки
    df_new["number"] = (
        df_new["text"].str.extract(r"Заявка №(\d+)", expand=False).str.strip()
    )

    # Дропнуть записи без номера заявки в теле сообщения (комментарии, уточнения и т.п.)
    df_new = df_new.dropna(subset="number")

    df_new["number"] = df_new["number"].astype(int)

    df_new["group"] = df_new["text"].apply(get_groups).apply(get_clean_data)
    df_new["theme"] = df_new["text"].apply(get_themes).apply(get_clean_data)
    df_new["status"] = df_new["text"].apply(get_status)
    df_new["url"] = df_new.apply(
        lambda row: "https://t.me/c/1806135606/" + str(row["id"]), axis=1
    )

    df_new = df_new[
        ["id", "url", "author", "number", "status", "group", "theme", "text"]
    ]

    df_new.to_excel(
        DOWNLOAD_FILE_PATH,
        index=False,
    )

    os.chmod(DOWNLOAD_FILE_PATH, 0o777)

    f.close()
