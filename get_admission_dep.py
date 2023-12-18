import metrics_collector.config as config
import metrics_collector.utils as utils
import metrics_collector.hospital as hospital
import metrics_collector.bi_emias as bi_emias
import json

# Настройки
PATH_TO_HOSPITAL_CREDENTIAL = r"/home/user/auth-hospital.json"
EXPORT_PATH = r"/etc/samba/share/download/Приемные отделения"


def start_hospital_export():
    with open(PATH_TO_HOSPITAL_CREDENTIAL) as f:
        data = json.load(f)

    auth_username = data["username"]
    auth_password = data["password"]

    hospital.authorize(auth_username, auth_password)
    hospital.load_admission_dep_report()
