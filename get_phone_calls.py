import os
import logging
import json
import metrics_collector.utils as utils

from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


CURRENT_PATH = os.path.abspath(os.getcwd())
PATH_TO_CREDENTIAL = r"/home/user/phone_calls_auth.json"
UPLOAD_PATH = r"/etc/samba/share/upload/"
DOWNLOAD_PATH = r"/etc/samba/share/download/"


def download_phone_calls():
    # Опции для веб-драйвера
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--headless=new")
    options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": CURRENT_PATH,
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

    logging.info("Начинается авторизация")

    # Очистить куки
    browser.delete_all_cookies()
    # Убедиться что открыта только одна вкладка
    if len(browser.window_handles) > 1:
        browser.switch_to.window(browser.window_handles[1])
        browser.close()
        browser.switch_to.window(browser.window_handles[0])

    browser.get(r"https://p1.cloudpbx.rt.ru/lk_new/#/login?redirect=%2Fadmin%2Fhistory")

    with open(PATH_TO_CREDENTIAL) as f:
        data = json.load(f)

    username_data = data["username"]
    password_data = data["password"]
    server_data = data["server"]

    WebDriverWait(browser, 30).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="username"]')))

    # Ввести логин
    element = browser.find_element(By.XPATH, '//*[@id="username"]')
    actions.click(element).key_down(Keys.CONTROL).send_keys("a").key_up(Keys.CONTROL).send_keys(
        username_data
    ).perform()
    # Ввести пароль
    element = browser.find_element(By.XPATH, '//*[@id="password"]')
    actions.click(element).key_down(Keys.CONTROL).send_keys("a").key_up(Keys.CONTROL).send_keys(
        password_data
    ).perform()
    # Ввести сервер
    element = browser.find_element(By.XPATH, '//*[@id="domain"]')
    actions.click(element).key_down(Keys.CONTROL).send_keys("a").key_up(Keys.CONTROL).send_keys(
        server_data
    ).perform()
    # Нажать на кнопку "Войти"
    browser.find_element(By.XPATH, '//*[@id="testform"]/div[4]/div[1]/button').click()

    logging.info("Авторизация завершена")
    logging.info("Открываю страницу отчета")
    browser.get("https://p1.cloudpbx.rt.ru/lk_new/#/admin/history")

    WebDriverWait(browser, 30).until(
        EC.invisibility_of_element(
            (By.XPATH, '//*[@id="page-content-wrapper"]/main/div/div[7]/div/div/div[1]/div[2]/svg')
        )
    )
    option = WebDriverWait(browser, 30).until(
        EC.visibility_of_element_located(
            (By.XPATH, '//*[@id="page-content-wrapper"]/main/div/div[5]/div[1]/a[3]')
        )
    )
    actions.move_to_element(option).click().perform()

    WebDriverWait(browser, 30).until(
        EC.invisibility_of_element(
            (By.XPATH, '//*[@id="page-content-wrapper"]/main/div/div[7]/div/div/div[1]/div[2]/svg')
        )
    )

    option = WebDriverWait(browser, 30).until(
        EC.visibility_of_element_located(
            (By.XPATH, '//*[@id="page-content-wrapper"]/main/div/div[6]/span[1]')
        )
    )
    actions.move_to_element(option).click().perform()

    logging.info("Начато сохранение файла")
    browser.save_screenshot(r'/etc/samba/share/upload/error.png')
    utils.download_wait(CURRENT_PATH, 600, len(os.listdir(CURRENT_PATH)) + 1)
    logging.info("Сохранение завершено")


def start_download_phone_calls():
    # Удалить устаревший файл с отчетом
    if not utils.is_actual_report_exist(UPLOAD_PATH + r"/Отчет по вызовам в домене.xlsx"):
        download_phone_calls()
