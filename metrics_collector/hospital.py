import metrics_collector.config as config
import metrics_collector.utils as utils
import os
import logging
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

browser = config.browser
actions = config.actions
reports_path = config.reports_path

def authorize(login_data: str, password_data: str):
    """
    Авторизация в ЕМИАС СТАЦИОНАР
    https://hospital.emias.mosreg.ru/?c=portal&m=promed&lang=ru
    """
    logging.info("Начинается авторизация в ЕМИАС Стационар")
    # Очистить куки
    browser.delete_all_cookies()
    # Убедиться что открыта только одна вкладка
    if len(browser.window_handles) > 1:
        browser.switch_to.window(browser.window_handles[1])
        browser.close()
        browser.switch_to.window(browser.window_handles[0])
    # Открыть страницу ввода логина и пароля
    browser.get("https://hospital.emias.mosreg.ru/?c=portal&m=promed&lang=ru")
    # Ввести логин
    login_field = browser.find_element(By.XPATH, '//*[@id="promed-login"]')
    actions.click(login_field).key_down(Keys.CONTROL).send_keys("a").key_up(Keys.CONTROL).send_keys(
        login_data
    ).perform()
    # Ввести пароль
    password_field = browser.find_element(By.XPATH, '//*[@id="promed-password"]')
    actions.click(password_field).key_down(Keys.CONTROL).send_keys("a").key_up(
        Keys.CONTROL
    ).send_keys(password_data).perform()
    # Нажать на кнопку "Войти"
    browser.find_element(By.XPATH, '//*[@id="auth_submit"]').click()
    logging.info("Авторизация пройдена")


def load_admission_dep_report():
    """
    Отчет Список пациентов, поступивших в приёмное отделение
    """
    logging.info("Открываются отчеты")
    WebDriverWait(browser, 180).until(EC.element_to_be_clickable((By.XPATH, '//*[@test_id="win_swLpuAdminWorkPlaceWindow_pnl___btn_Plani_flyuorograficheskih_meropriyatiy"]')))

    element = browser.find_element(By.XPATH, '//*[@test_id="tbr_btn_Otcheti"]')
    element.click()
    element = browser.find_element(By.XPATH, '//*[@test_id="mi_Statisticheskaya_otchetnost"]')
    element.click()

    WebDriverWait(browser, 60).until(EC.visibility_of_element_located((By.XPATH, '//*[@test_id="win_swReportEndUserWindow_pnl_Katalog_otchetov"]')))
    WebDriverWait(browser, 30).until(EC.visibility_of_element_located((By.XPATH, '/html/body/div[1]/div/div[2]/div[5]/div[2]/div[1]/div/div/div/div[2]/div[2]/div[2]/ul/div/li[5]/div/img[1]')))

    element = browser.find_element(By.XPATH, "/html/body/div[1]/div/div[2]/div[5]/div[2]/div[1]/div/div/div/div[2]/div[2]/div[2]/ul/div/li[5]/div/img[1]")
    element.click()
    WebDriverWait(browser, 30).until(EC.visibility_of_element_located((By.XPATH, '/html/body/div[1]/div/div[2]/div[5]/div[2]/div[1]/div/div/div/div[2]/div[2]/div[2]/ul/div/li[5]/ul/li/div/a')))
    element = browser.find_element(By.XPATH, "/html/body/div[1]/div/div[2]/div[5]/div[2]/div[1]/div/div/div/div[2]/div[2]/div[2]/ul/div/li[5]/ul/li/div/a")
    element.click()
    WebDriverWait(browser, 30).until(EC.element_to_be_clickable((By.XPATH, '//*[@test_id="win_swReportEndUserWindow_btn_Sformirovat_otchet"]')))
    element = browser.find_element(By.XPATH, '//*[@test_id="win_swReportEndUserWindow_btn_Sformirovat_otchet"]')
    element.click()
    logging.info("Начинается сохранение отчета")
    utils.download_wait(reports_path, 600, len(os.listdir(reports_path)) + 1)
    logging.info("Сохранение завершено")
