import metrics_collector.config as config
import metrics_collector.utils as utils
import metrics_collector.hospital as hospital
import metrics_collector.bi_emias as bi_emias
from selenium.common.exceptions import TimeoutException
import json
import os

# Настройки
PATH_TO_HOSPITAL_CREDENTIAL = r"/home/user/auth-hospital.json"
PATH_TO_BI_CREDENTIAL = r"/home/user/auth-bi-emias.json"
EXPORT_PATH = r"/etc/samba/share/download/Приемные отделения"


def start_hospital_export():
    with open(PATH_TO_HOSPITAL_CREDENTIAL) as f:
        data = json.load(f)

    auth_username = data["username"]
    auth_password = data["password"]

    try:
        hospital.authorize(auth_username, auth_password)
        hospital.load_admission_dep_report()
    except TimeoutException as ex:
        config.browser.save_screenshot(os.path.join(EXPORT_PATH, "hospital_error.png"))
        raise ex


def start_bi_export():
    # Создать папку для выгрузки
    try:
        os.mkdir(EXPORT_PATH)
    except FileExistsError:
        pass
    if not utils.is_actual_report_exist(
        config.reports_path + "Дашборд приемного отделения.xlsx"
    ):
        # Получить путь к файлу с данными для авторизации
        f = open(PATH_TO_BI_CREDENTIAL, "r", encoding="utf-8")
        data = json.load(f)
        f.close()
        for _departments in data["departments"]:
            for _units in _departments["units"]:
                bi_emias.authorize(_units["login"], _units["password"])
        # Выгрузка отчета
        try:
            bi_emias.load_any_report(
                "dashboard_priem_otdel_krasnogorsk_al", use_dates=False
            )
            bi_emias.export_report()
        except TimeoutException:
            config.browser.save_screenshot(os.path.join(EXPORT_PATH, "bi_error.png"))
            raise TimeoutException
