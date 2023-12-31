import os

from datetime import date, timedelta
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service

reports_path = r"/etc/samba/share/download"

# Опции для веб-драйвера
options = webdriver.ChromeOptions()
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--start-maximized")
options.add_argument("--disable-extensions")
options.add_argument("--disable-popup-blocking")
#options.add_argument("--disable-gpu")
#options.add_argument("--disable-dev-shm-usage")
#options.add_argument("--disable-browser-side-navigation")
options.add_argument("--headless=new")
options.add_experimental_option(
    "prefs",
    {
        "download.default_directory": reports_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False,
        "safebrowsing.disable_download_protection": True,
    },
)

# Выбираем драйвер браузера и устанавливаем его опции
service = Service(r"/home/user/chromedriver")
browser = webdriver.Chrome(options=options, service=service)
actions = ActionChains(browser)

# Очистить кэш, сессии, хранилище
#browser.execute_cdp_cmd(
#   "Storage.clearDataForOrigin",
#    {
#        "origin": "*",
#        "storageTypes": "all",
#    },
#)

# Период: с начала недели по сегодняшний день
first_date = date.today() - timedelta(days=date.today().weekday())
last_date = date.today()
yesterday_date = date.today() - timedelta(days=1)

# Если сегодня понедельник, то берем всю прошлую неделю
monday = date.today() - timedelta(days=date.today().weekday())
if date.today() == monday:
    first_date = monday - timedelta(days=7)  # начало прошлой недели
