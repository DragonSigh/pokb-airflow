# Импорт библиотек и классов
from datetime import date, timedelta
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchDriverException

# Директория для выгрузки отчетов
reports_path = r"/etc/samba/share/download"

# Опции для веб-драйвера
options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
options.add_argument("--enable-automation")
options.add_argument("--enable-javascript")
options.add_argument("--ignore-certificate-errors")
options.add_argument("--ignore-ssl-errors")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--disable-extensions")
options.add_argument("--disable-popup-blocking")
options.add_argument("--disable-gpu")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-browser-side-navigation")
options.add_argument("--disable-default-apps")
options.add_argument("--disable-web-security")
options.add_argument("--allow-running-insecure-content")
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
# options.add_argument("--enable-features=NetworkServiceInProcess")
# options.add_argument("--disable-features=NetworkService")
# Cache
# options.add_argument("--incognito")
# options.add_argument("--aggressive-cache-discard")
# options.add_argument("--disable-cache")
# options.add_argument("--disable-application-cache")
# options.add_argument("--disable-offline-load-stale-cache")
# options.add_argument("--disk-cache-size=0")
options.add_experimental_option(
    "prefs",
    {
        "download.default_directory": reports_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False,
        "safebrowsing.disable_download_protection": True,
        "https-upgrades": False,
    },
)

# Выбираем драйвер браузера и устанавливаем его опции
try:
    service = Service(r"/home/user/chromedriver")
    browser = webdriver.Chrome(options=options, service=service)
except NoSuchDriverException:
    service = Service(r"C:\chromedriver\chromedriver.exe")
    browser = webdriver.Chrome(options=options, service=service)
    reports_path = r"E:\System\Projects\pokb-metrics-collector\reports"

# browser.set_page_load_timeout(30)
# browser.set_script_timeout(3)
actions = ActionChains(browser)

# Очистить кэш, сессии, хранилище
browser.execute_cdp_cmd(
    "Storage.clearDataForOrigin",
    {
        "origin": "*",
        "storageTypes": "all",
    },
)

# Очистить хранилище HSTS
browser.get("chrome://net-internals/#hsts")
element = browser.find_element(
    By.XPATH, '//*[@id="domain-security-policy-view-delete-input"]'
)

actions.click(element).send_keys("llo.emias.mosreg.ru").send_keys(Keys.ENTER).perform()

# Период: с начала недели по сегодняшний день
first_date = date.today() - timedelta(days=date.today().weekday())
last_date = date.today()
yesterday_date = date.today() - timedelta(days=1)

# Если сегодня понедельник, то берем всю прошлую неделю
monday = date.today() - timedelta(days=date.today().weekday())
if date.today() == monday:
    first_date = monday - timedelta(days=7)  # начало прошлой недели
