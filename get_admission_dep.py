import metrics_collector.config as config
import metrics_collector.utils as utils
import metrics_collector.hospital as hospital
import metrics_collector.bi_emias as bi_emias
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import json
import os

# Настройки
PATH_TO_HOSPITAL_CREDENTIAL = r"/home/user/auth-hospital.json"
PATH_TO_BI_CREDENTIAL = r"/home/user/auth-bi-emias.json"
EXPORT_PATH = r"/etc/samba/share/download/Приемные отделения"


def start_hospital_export():
    # Создать папку для выгрузки
    try:
        os.mkdir(EXPORT_PATH)
    except FileExistsError:
        pass
    if (
        utils.is_actual_report_exist(config.reports_path, "r50_han_PriemGosp_pg", 4)
        is None
    ):
        with open(PATH_TO_HOSPITAL_CREDENTIAL) as f:
            data = json.load(f)

        auth_username = data["username"]
        auth_password = data["password"]

        try:
            hospital.authorize(auth_username, auth_password)
            hospital.load_admission_dep_report()
            config.browser.quit()
        except Exception as ex:
            config.browser.save_screenshot(
                os.path.join(EXPORT_PATH, "hospital_error.png")
            )
            raise ex


def start_bi_export():
    # Создать папку для выгрузки
    try:
        os.mkdir(EXPORT_PATH)
    except FileExistsError:
        pass
    if (
        utils.is_actual_report_exist(
            config.reports_path, "Дашборд приемного отделения", 4
        )
        is None
    ):
        with open(PATH_TO_BI_CREDENTIAL) as f:
            data = json.load(f)
        auth_username = data["username"]
        auth_password = data["password"]

        bi_emias.authorize(auth_username, auth_password)

        # Выгрузка отчета
        try:
            bi_emias.load_any_report(
                "dashboard_priem_otdel_krasnogorsk_al", use_dates=False
            )
            bi_emias.export_report()
            config.browser.quit()
        except Exception as ex:
            config.browser.save_screenshot(os.path.join(EXPORT_PATH, "bi_error.png"))
            raise ex


def start_analyze():
    import pandas as pd
    import numpy as np
    import re

    df = pd.read_excel(
        os.path.join(config.reports_path, "Дашборд приемного отделения.xlsx"),
        skiprows=[0, 2],
    )
    df_emias = pd.DataFrame()

    # Только Подольская ОКБ
    df = df[(df["ОГРН"] == 1215000036305)]

    for filename in os.listdir(config.reports_path):
        if "r50_han_PriemGosp_pg" in filename:
            file_path = os.path.join(config.reports_path, filename)
            if os.path.isfile(file_path):
                df_emias = pd.read_excel(
                    file_path,
                    skiprows=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12],
                    skipfooter=1,
                )

    df = df.merge(df_emias, on="Номер карты", how="left")

    df["ОСП"] = df["Приемное отделение"].apply(
        lambda x: re.search(r"ОСП \d", x)[0]
        if re.match(r"^.*ОСП \d$", x)
        else "Кирова 38"
    )

    df = df[
        [
            "Номер карты",
            "ФИО",
            "Приемное отделение",
            "ФИО Врача",
            "ОСП",
            "Дата поступления",
            "Время поступления",
            "Диагноз",
            "Состояние при поступлении",
            "Время проведения первичного осмотра из Документа в ЭМК",
            "Разница между временем поступления и временем проведения первичного осмотра",
            "Направление на лабораторные исследования выписано",
            "Направление на ЭКГ выписано",
            "Направление на инструментальные исследования выписано",
            "Профильное отделение",
            "Время перевода в профильное отделение или выбытие",
            "Время пребывания в приемном, мин, от времени проведения первичного осмотра до перевода в профильное отделение (при отсутствии данных по первичному осмотру учитывается дата поступления)",
            "Время пребывания в приемном, мин, от времени поступления до перевода в профильное отделение",
        ]
    ]

    df["po_done"] = ~df["Время проведения первичного осмотра из Документа в ЭМК"].isna()
    df["napr_done"] = (
        ~df["Направление на лабораторные исследования выписано"].isna()
        + ~df["Направление на ЭКГ выписано"].isna()
        + ~df["Направление на инструментальные исследования выписано"].isna()
    ) > 0

    df["max_time"] = np.where((df["Состояние при поступлении"] == "Удовлет"), 60, 30)

    df["correct_time"] = (
        df[
            "Разница между временем поступления и временем проведения первичного осмотра"
        ]
        < df[
            "Время пребывания в приемном, мин, от времени поступления до перевода в профильное отделение"
        ]
    ) & (
        df[
            "Разница между временем поступления и временем проведения первичного осмотра"
        ]
        <= df["max_time"]
    )

    df = df[df["Профильное отделение"] != "А и Реан"]

    df["Заполнен осмотр"] = (
        df.po_done.astype(str).str.replace("True", "Да").str.replace("False", "Нет")
    )
    df["Создано направление"] = (
        df.napr_done.astype(str).str.replace("True", "Да").str.replace("False", "Нет")
    )
    df["Время осмотра корректно"] = (
        df.correct_time.astype(str)
        .str.replace("True", "Да")
        .str.replace("False", "Нет")
    )

    names = {
        "Приемное отделение №1 (стационар) Кирова 38 общее": "Общее",
        "Приемное отделение №2 (кардиология) Кирова 38-13 корпус": "13 корпус",
        "Приемное отделение №5 (РСЦ) Кирова 38": "РСЦ",
        "Приемное отделение №2 (стационар) ОСП 2": "ОСП 2",
        "Приемное отделение №3 (стационар) ОСП 3": "ОСП 3",
        "Приемное отделение №4 (стационар) ОСП 5": "ОСП 5",
        "Приемное отделение №6 (стационар) ОСП 7": "ОСП 7",
    }

    df_perv = (
        df.query('ОСП == "Кирова 38"')
        .groupby(["ФИО Врача", "Приемное отделение"])
        .agg(
            {
                "ФИО": "count",
                "po_done": "sum",
                "napr_done": "sum",
                "correct_time": "sum",
            }
        )
        .assign(
            Заполнен_осмотр=lambda x: round(100 * x["po_done"] / x["ФИО"], 0).astype(
                int
            ),
            Создано_направление=lambda x: round(
                100 * x["napr_done"] / x["ФИО"], 0
            ).astype(int),
            Время_осмотра_корректно=lambda x: round(
                100 * x["correct_time"] / x["ФИО"], 0
            ).astype(int),
        )[["ФИО", "Заполнен_осмотр", "Создано_направление", "Время_осмотра_корректно"]]
        .reset_index()
        .rename(
            columns={
                "ФИО": "Пациентов",
                "Заполнен_осмотр": "Заполнен осмотр, %",
                "Создано_направление": "Создано направление, %",
                "Время_осмотра_корректно": "Время осмотра корректно, %",
            }
        )
        .assign(Приемное_отделение=lambda x: x["Приемное отделение"].map(names))[
            [
                "ФИО Врача",
                "Приемное_отделение",
                "Пациентов",
                "Заполнен осмотр, %",
                "Создано направление, %",
                "Время осмотра корректно, %",
            ]
        ]
    )

    utils.save_to_excel(
        df_perv,
        os.path.join(
            EXPORT_PATH, "Заполнение первичного осмотра в приемном по врачам.xlsx"
        ),
    )

    df_perv_osp = (
        df.query('ОСП != "Кирова 38"')
        .groupby(["ФИО Врача", "Приемное отделение"])
        .agg(
            {
                "ФИО": "count",
                "po_done": "sum",
                "napr_done": "sum",
                "correct_time": "sum",
            }
        )
        .assign(
            Заполнен_осмотр=lambda x: round(100 * x["po_done"] / x["ФИО"], 0).astype(
                int
            ),
            Создано_направление=lambda x: round(
                100 * x["napr_done"] / x["ФИО"], 0
            ).astype(int),
            Время_осмотра_корректно=lambda x: round(
                100 * x["correct_time"] / x["ФИО"], 0
            ).astype(int),
        )[["ФИО", "Заполнен_осмотр", "Создано_направление", "Время_осмотра_корректно"]]
        .reset_index()
        .rename(
            columns={
                "ФИО": "Пациентов",
                "Заполнен_осмотр": "Заполнен осмотр, %",
                "Создано_направление": "Создано направление, %",
                "Время_осмотра_корректно": "Время осмотра корректно, %",
            }
        )
        .assign(Приемное_отделение=lambda x: x["Приемное отделение"].map(names))[
            [
                "ФИО Врача",
                "Приемное_отделение",
                "Пациентов",
                "Заполнен осмотр, %",
                "Создано направление, %",
                "Время осмотра корректно, %",
            ]
        ]
    )

    utils.save_to_excel(
        df_perv_osp,
        os.path.join(
            EXPORT_PATH, "ОСП Заполнение первичного осмотра в приемном по врачам.xlsx"
        ),
    )

    for i in df["Приемное отделение"].unique():
        df_temp = df[
            (df["Приемное отделение"] == i)
            & (~df.po_done | ~df.napr_done | ~df.correct_time)
        ][
            [
                "Номер карты",
                "ФИО",
                "Дата поступления",
                "ФИО Врача",
                "Диагноз",
                "Профильное отделение",
                "ОСП",
                "Заполнен осмотр",
                "Создано направление",
                "Время осмотра корректно",
            ]
        ].sort_values(
            "ФИО Врача"
        )

        utils.save_to_excel(
            df_temp,
            os.path.join(EXPORT_PATH, f"{i[:22]}.xlsx"),
        )

        dataframe_to_pdf(df_temp, os.path.join(EXPORT_PATH, f"{i[:22]}.xlsx"))

    df_stat = (
        df.query('ОСП == "Кирова 38"')
        .groupby("Приемное отделение")
        .agg(
            {
                "Время пребывания в приемном, мин, от времени поступления до перевода в профильное отделение": "mean"
            }
        )
        .apply(round)
        .rename(
            columns={
                "Время пребывания в приемном, мин, от времени поступления до перевода в профильное отделение": "Среднее время пребывания в приемном"
            }
        )
        .reset_index()
    )

    utils.save_to_excel(
        df_stat,
        os.path.join(EXPORT_PATH, "Статистика по приемным отделениям.xlsx"),
    )

    df_stat_osp = (
        df.query('ОСП != "Кирова 38"')
        .groupby("Приемное отделение")
        .agg(
            {
                "Время пребывания в приемном, мин, от времени поступления до перевода в профильное отделение": "mean"
            }
        )
        .apply(round)
        .rename(
            columns={
                "Время пребывания в приемном, мин, от времени поступления до перевода в профильное отделение": "Среднее время пребывания в приемном"
            }
        )
        .reset_index()
    )

    utils.save_to_excel(
        df_stat_osp,
        os.path.join(EXPORT_PATH, "ОСП Статистика по приемным отделениям.xlsx"),
    )


def _draw_as_table(df, pagesize):
    alternating_colors = [
        ["white"] * len(df.columns),
        ["lightgray"] * len(df.columns),
    ] * len(df)
    alternating_colors = alternating_colors[: len(df)]
    fig, ax = plt.subplots(figsize=pagesize)
    ax.axis("tight")
    ax.axis("off")
    the_table = ax.table(
        cellText=df.values,
        rowLabels=df.index,
        colLabels=df.columns,
        rowColours=["lightblue"] * len(df),
        colColours=["lightblue"] * len(df.columns),
        cellColours=alternating_colors,
        loc="center",
    )
    return fig


def dataframe_to_pdf(df, filename, numpages=(1, 1), pagesize=(11, 8.5)):
    with PdfPages(filename) as pdf:
        nh, nv = numpages
        rows_per_page = len(df) // nh
        cols_per_page = len(df.columns) // nv
        for i in range(0, nh):
            for j in range(0, nv):
                page = df.iloc[
                    (i * rows_per_page) : min((i + 1) * rows_per_page, len(df)),
                    (j * cols_per_page) : min((j + 1) * cols_per_page, len(df.columns)),
                ]
                fig = _draw_as_table(page, pagesize)
                if nh > 1 or nv > 1:
                    # Add a part/page number at bottom-center of page
                    fig.text(
                        0.5,
                        0.5 / pagesize[0],
                        "Part-{}x{}: Page-{}".format(i + 1, j + 1, i * nv + j + 1),
                        ha="center",
                        fontsize=8,
                    )
                pdf.savefig(fig, bbox_inches="tight")

                plt.close()
