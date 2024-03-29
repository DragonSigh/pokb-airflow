import metrics_collector.config as config
import metrics_collector.utils as utils
import metrics_collector.emias as emias
import metrics_collector.kornet as kornet
from selenium.common.exceptions import TimeoutException
import os
import json
import pandas as pd
import logging

# Настройки
PATH_TO_CREDENTIAL = r"/home/user/"
EXPORT_PATH = r"/etc/samba/share/download/Показатель 22"

first_date = config.first_date
last_date = config.last_date
yesterday_date = config.yesterday_date


# Функция для нормализации ФИО из ЕМИАС
def complex_rename(x):
    if isinstance(x, str):
        first_name = x.split(" ")[1]
        second_name = x.split(" ")[2]
        last_name = x.split(" ")[3].replace(",", "")
        return f"{first_name} {second_name} {last_name}".upper()
    else:
        return 0


def start_kornet_report_saving():
    # Создать папку для выгрузки метрики
    # try:
    #    os.mkdir(EXPORT_PATH)
    # except FileExistsError:
    #    pass
    # Получить путь к файлу с данными для авторизации
    kornet_credentials_path = os.path.join(PATH_TO_CREDENTIAL, "auth-kornet.json")
    # Определение дат
    first_date = config.first_date
    last_date = config.last_date
    logging.info(
        f'Выбран период: с {first_date.strftime("%d.%m.%Y")} '
        f'по {last_date.strftime("%d.%m.%Y")}'
    )
    logging.info(f"Путь выгрузки отчётов: {EXPORT_PATH}")
    f = open(kornet_credentials_path, "r", encoding="utf-8")
    data = json.load(f)
    f.close()
    df_list = []
    for _departments in data["departments"]:
        logging.info(
            f"Начинается сохранение отчёта для подразделения: "
            f'{_departments["department"]}'
        )
        for _units in _departments["units"]:
            logging.info("Авторизация в отделение: " f'{_units["name"]}')
            kornet.authorize(_units["login"], _units["password"])
            kornet.load_dlo_report(first_date, last_date)
            kornet.export_report()
            exported_file = utils.get_newest_file(config.reports_path)

            df_temp = pd.read_excel(
                exported_file,
                skiprows=range(1, 12),
                skipfooter=18,
                usecols="C,D,E,I,L,N,P,S,X",
            )
            df_temp.insert(0, "Отделение", _units["name"])
            df_list.append(df_temp)
            os.remove(exported_file)
    df_kornet = pd.concat(df_list)
    df_kornet.columns = [
        "Отделение",
        "Серия и номер",
        "Дата выписки",
        "ФИО врача",
        "СНИЛС",
        "ФИО пациента",
        "Код категории",
        "Адрес",
        "Препарат",
        "Количество",
    ]
    logging.info("Выгрузка из КОРНЕТА завершена")
    utils.save_to_excel(df_kornet, os.path.join(EXPORT_PATH, "Промежуточный КОРНЕТ.xlsx"))


def start_emias_report_saving():
    # Получить путь к файлу с данными для авторизации
    emias_credentials_path = os.path.join(PATH_TO_CREDENTIAL, "auth-emias.json")
    # Определение дат
    first_date = config.first_date
    last_date = config.last_date
    # Открываем данные для авторизации и проходим по списку кабинетов
    logging.info(
        "Выбран период: с "
        f'{first_date.strftime("%d.%m.%Y")} '
        f'по {last_date.strftime("%d.%m.%Y")}'
    )
    f = open(emias_credentials_path, "r", encoding="utf-8")
    data = json.load(f)
    f.close()
    for _departments in data["departments"]:
        logging.info("Начинается сохранение отчёта из ЕМИАС")
        for _units in _departments["units"]:
            logging.info("Авторизация в ЕМИАС")
            emias.authorize(_units["login"], _units["password"])
    # ID кабинетов выписки лекарств
    cabinets_list = [
        "2434",
        "2460",
        "2459",
        "2450",
        "636",
        "2458",
        "2343",
        "2457",
        "2449",
        "2711",
    ]
    df_list = []
    for cabinet in cabinets_list:
        emias.load_system_report(cabinet, first_date, last_date)
        emias.export_system_report(cabinet)
        exported_file = utils.get_newest_file(config.reports_path)
        df_temp = pd.read_excel(
            exported_file,
            usecols="A, C, H, K, O",
            skiprows=range(1, 3),
            skipfooter=4,
            header=0,
        )
        df_list.append(df_temp)
        os.remove(exported_file)
    df_emias = pd.concat(df_list)
    df_emias.columns = [
        "Подразделение",
        "Кабинет",
        "ФИО пациента",
        "Время приема по записи",
        "Отметка о приеме",
    ]
    logging.info("Выгрузка из ЕМИАС завершена")
    utils.save_to_excel(df_emias, EXPORT_PATH + "/Промежуточный ЕМИАС.xlsx")


def analyze_data(df_kornet, df_emias):

    # Очистить папку с предыдущими отчетами
    utils.emptydir(EXPORT_PATH, exclude=["Промежуточный ЕМИАС.xlsx", "Промежуточный КОРНЕТ.xlsx"])

    df_emias["ФИО пациента"] = df_emias["ФИО пациента"].apply(complex_rename)

    # Очистка датафрейма ЕМИАСа
    values_to_remove = ["Максимум", "Итог", "Среднее", "Количество"]
    pattern = "|".join(values_to_remove)
    df_emias = df_emias.loc[
        ~df_emias["Подразделение"].str.contains(pattern, case=False)
    ]

    # Сконвертировать время приема по записи в дату
    df_emias["Время приема по записи"] = pd.to_datetime(
        df_emias["Время приема по записи"]
    )
    # Выделить столбец только с датой
    df_emias["Дата записи"] = pd.to_datetime(
        df_emias["Время приема по записи"], dayfirst=True
    ).dt.date
    # Сгруппировать по ФИО и дате записи, если есть дубли
    df_emias = (
        df_emias.groupby(["ФИО пациента", "Дата записи"]).head(1).reset_index(drop=True)
    )

    # Выделить в отдельный датафрейм все фактические неявки за прошедшие дни
    df_noshow = df_emias[
        (df_emias["Отметка о приеме"] == "Неявка")
        & (df_emias["Время приема по записи"] < pd.to_datetime("today").normalize())
    ]
    # Убрать все фактические неявки из основного датафрейма ЕМИАС
    df_emias = df_emias[
        ~(
            (df_emias["Отметка о приеме"] == "Неявка")
            & (df_emias["Время приема по записи"] < pd.to_datetime("today").normalize())
        )
    ]
    # Убрать пустые значения, отмены и переносы
    df_emias = df_emias[
        (~df_emias["Отметка о приеме"].isnull())
        & (df_emias["Отметка о приеме"] != "Запись отменена")
        & (df_emias["Отметка о приеме"] != "Запись перенесена")
    ]
    # Пометить все неявки в будущем как запланированные посещения
    df_emias.loc[
        df_emias["Отметка о приеме"] == "Неявка", "Отметка о приеме"
    ] = "Запланирован"

    # Поиск ФИО, которых нет в записи в кабинет ЕМИАС

    df_kornet["ФИО пациента"] = df_kornet["ФИО пациента"].str.upper()
    df_kornet = (
        df_kornet.groupby(["ФИО пациента", "Дата выписки"])
        .head(1)
        .reset_index(drop=True)
    )
    df_kornet["Дата выписки"] = pd.to_datetime(
        df_kornet["Дата выписки"], dayfirst=True
    ).dt.date

    df_kornet["Подразделение"] = df_kornet["Отделение"].apply(utils.get_department)

    result = df_kornet[~df_kornet["ФИО пациента"].isin(df_emias["ФИО пациента"])]
    result = result[result["Дата выписки"] >= last_date]

    for department in result["Подразделение"].unique():
        df_temp = result[result["Подразделение"] == department].drop(
            ["Подразделение"], axis=1
        )
        # Фильтрация датафрейма по уникальному значению в колонке
        file_path = os.path.join(
            EXPORT_PATH,
            department
            + " - нет записи в кабинет выписки рецептов на "
            + str(last_date)
            + ".xlsx",
        )
        utils.save_to_excel(
            df_temp,
            file_path,
        )
        os.chmod(file_path, 0o777)

    # Поиск людей, которым не отмечена явка в ЕМИАС, но выдан рецепт в Корнет
    # Соединение датафреймов неявок и корнета
    df_noshow = df_noshow.merge(
        df_kornet,
        left_on=["ФИО пациента", "Дата записи"],
        right_on=["ФИО пациента", "Дата выписки"],
        how="inner",
    ).drop(
        ["Отметка о приеме", "Отделение", "СНИЛС", "Дата записи", "Дата выписки"],
        axis=1,
    )

    # Наглядное выражение времени вне расписания
    df_noshow["Время приема по записи"] = df_noshow["Время приема по записи"].apply(
        lambda x: x.strftime("%Y-%m-%d %H:%M").replace("00:00", "вне расписания")
    )
    # Сохранить отчет по непроставленным явкам
    utils.save_to_excel(
        df_noshow,
        EXPORT_PATH + "/_Не проставлена явка о приеме, но выписан рецепт.xlsx",
    )
    os.chmod(
        EXPORT_PATH + "/_Не проставлена явка о приеме, но выписан рецепт.xlsx", 0o777
    )
    # Сегодняшний день исключаем, так как рецепт ещё могут выписать позже
    df_kornet = df_kornet[df_kornet["Дата выписки"] < last_date]
    # ЕСЛИ НУЖНО сохранить объединенные отчёты для обеих систем для дебага
    # save_to_excel(df_kornet, reports_path + '\\result\\' +'КОРНЕТ.xlsx', index_arg=True)
    # save_to_excel(df_emias, reports_path + '\\result\\' +'ЕМИАС.xlsx', index_arg=True)

    # Сводим статистику по выписанными не по регламенту рецептам в единую таблицу
    df_kornet = (
        df_kornet.merge(
            df_emias,
            left_on=["ФИО пациента", "Дата выписки"],
            right_on=["ФИО пациента", "Дата записи"],
            how="left",
        )
        .assign(reglament=lambda x: ~x["Время приема по записи"].isna())
        .groupby("Отделение")
        .agg({"reglament": ["count", "sum"]})
        .assign(
            rate_correct=lambda x: round(
                100 * x["reglament"]["sum"] / x["reglament"]["count"]
            )
        )
    )

    # Сохраняем свод
    df_kornet.columns = ["Всего рецептов", "Из них по регламенту", "% по регламенту"]
    file_path = os.path.join(
        EXPORT_PATH,
        "_Свод по выписанным рецептам не по регламенту "
        + str(first_date)
        + "_"
        + str(yesterday_date)
        + ".xlsx",
    )
    utils.save_to_excel(
        df_kornet,
        EXPORT_PATH
        + "/_Свод по выписанным рецептам не по регламенту "
        + str(first_date)
        + "_"
        + str(yesterday_date)
        + ".xlsx",
        index_arg=True,
    )
    os.chmod(file_path, 0o777)

    # Аггрегация для дашборда

    df_kornet = df_kornet.reset_index()
    df_kornet["Подразделение"] = df_kornet["Отделение"].apply(utils.get_department)
    df_kornet = df_kornet.groupby("Подразделение").sum()
    df_kornet.loc["ПОКБ"] = df_kornet.sum(numeric_only=True)
    df_kornet["% по показателю 22"] = round(
        df_kornet["Из них по регламенту"] / df_kornet["Всего рецептов"] * 100
    )
    df_kornet = df_kornet.drop(
        ["Отделение", "Из них по регламенту", "Всего рецептов", "% по регламенту"],
        axis=1,
    ).reset_index()
    utils.save_to_excel(
        df_kornet, os.path.join(EXPORT_PATH, "agg_22.xlsx"), index_arg=False
    )
    os.chmod(os.path.join(EXPORT_PATH, "agg_22.xlsx"), 0o777)


def check_metric_022():
    # Выгрузка отчета
    try:
        if (
            not utils.is_actual_report_exist(EXPORT_PATH, "Промежуточный КОРНЕТ")
            is None
        ):
            logging.info("Промежуточный отчет из КОРНЕТ уже есть в папке")
        else:
            start_kornet_report_saving()
        df_kornet = pd.read_excel(os.path.join(EXPORT_PATH, "Промежуточный КОРНЕТ.xlsx"), header=0)
        if not utils.is_actual_report_exist(EXPORT_PATH, "Промежуточный ЕМИАС") is None:
            logging.info("Промежуточный отчет из ЕМИАС уже есть в папке")
        else:
            start_emias_report_saving()
        df_emias = pd.read_excel(
            os.path.join(EXPORT_PATH, "Промежуточный ЕМИАС.xlsx"), header=0
        )
    except Exception as ex:
        config.browser.save_screenshot(os.path.join(EXPORT_PATH, "error.png"))
        raise ex
    analyze_data(df_kornet, df_emias)
