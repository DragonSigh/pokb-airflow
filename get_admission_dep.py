import metrics_collector.config as config
import metrics_collector.utils as utils
import metrics_collector.hospital as hospital
import metrics_collector.bi_emias as bi_emias
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

def start_analyse():
    import pandas as pd
    import numpy as np
    import re

    df = pd.read_excel(os.path.join(config.reports_path, "Дашборд приемного отделения.xlsx"), skiprows=[0,2])
    df_emias = pd.DataFrame()

    for filename in os.listdir(config.reports_path):
        if "r50_han_PriemGosp_pg" in filename:
            file_path = os.path.join(config.reports_path, filename)
            if os.path.isfile(file_path):
                df_emias = pd.read_excel(file_path, skiprows=[0,1,2,3,4,5,6,7,8,9,10,12], skipfooter=1)

    df = df.merge(df_emias, on='Номер карты', how='left')

    df['ОСП'] = df['Приемное отделение'] \
                            .apply(lambda x: re.search('ОСП \d', x)[0] if re.match('^.*ОСП \d$', x) else 'Кирова 38')

    df = df[['Номер карты', 'ФИО', 'Приемное отделение', 'ФИО Врача', 'ОСП',
           'Дата поступления', 'Время поступления', 'Диагноз',
           'Состояние при поступлении',
           'Время проведения первичного осмотра из Документа в ЭМК',
           'Разница между временем поступления и временем проведения первичного осмотра',
           'Направление на лабораторные исследования выписано',
           'Направление на ЭКГ выписано',
           'Направление на инструментальные исследования выписано',
           'Профильное отделение',
           'Время перевода в профильное отделение или выбытие',
           'Время пребывания в приемном, мин, от времени проведения первичного осмотра до перевода в профильное отделение (при отсутствии данных по первичному осмотру учитывается дата поступления)',
           'Время пребывания в приемном, мин, от времени поступления до перевода в профильное отделение']]

    df['po_done'] = ~df['Время проведения первичного осмотра из Документа в ЭМК'].isna()
    df['napr_done'] = (~df['Направление на лабораторные исследования выписано'].isna() +
                      ~df['Направление на ЭКГ выписано'].isna() +
                      ~df['Направление на инструментальные исследования выписано'].isna()) > 0

    df['max_time'] = np.where((df['Состояние при поступлении'] == 'Удовлет'), 60, 30)

    df['correct_time'] = \
        (df['Разница между временем поступления и временем проведения первичного осмотра'] < \
         df['Время пребывания в приемном, мин, от времени поступления до перевода в профильное отделение']) \
            & \
        (df['Разница между временем поступления и временем проведения первичного осмотра'] <= \
         df['max_time'])

    df = df[df['Профильное отделение'] != 'А и Реан']

    df['Заполнен осмотр'] = df.po_done.astype(str).str.replace('True', 'Да').str.replace('False', 'Нет')
    df['Создано направление'] = df.napr_done.astype(str).str.replace('True', 'Да').str.replace('False', 'Нет')
    df['Время осмотра корректно'] = df.correct_time.astype(str).str.replace('True', 'Да').str.replace('False', 'Нет')

    names = {'Приемное отделение №3 (стационар) Кирова 38-экстренный 2 корп': '2 корпус',
         'Приемное отделение №1 (стационар) Кирова 38-18 корпус': '18 корпус',
         'Приемное отделение №10 (стационар) ОСП 7': 'ОСП 7',
         'Приемное отделение №8 (стационар) ОСП 5': 'ОСП 5',
         'Приемное отделение №5 (РСЦ) Кирова 38': 'РСЦ',
         'Приемное отделение №6 (стационар) ОСП 2': 'ОСП 2',
         'Приемное отделение №2 (кардиология) Кирова 38-13 корпус': '13 корпус'}  

    for i in df['Приемное отделение'].unique():
        df[(df["Приемное отделение"] == i) & (~df.po_done | ~df.napr_done | ~df.correct_time)] \
                [['Номер карты', 'ФИО', 'Дата поступления', 'ФИО Врача', 'Диагноз', 
                'Профильное отделение', 'ОСП', 'Заполнен осмотр', 'Создано направление', 'Время осмотра корректно']] \
            .sort_values('ФИО Врача') \
            .to_excel(os.join(EXPORT_PATH, f'{i[:22]}.xlsx'), index=False)
        