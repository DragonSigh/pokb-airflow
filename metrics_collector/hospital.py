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
    actions.click(login_field).key_down(Keys.CONTROL).send_keys("a").key_up(
        Keys.CONTROL
    ).send_keys(login_data).perform()
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
    WebDriverWait(browser, 360).until(
        EC.element_to_be_clickable(
            (
                By.XPATH,
                '//*[@test_id="win_swLpuAdminWorkPlaceWindow_pnl___btn_Plani_flyuorograficheskih_meropriyatiy"]',
            )
        )
    )

    element = browser.find_element(By.XPATH, '//*[@test_id="tbr_btn_Otcheti"]')
    element.click()
    element = browser.find_element(
        By.XPATH, '//*[@test_id="mi_Statisticheskaya_otchetnost"]'
    )
    element.click()

    WebDriverWait(browser, 60).until(
        EC.visibility_of_element_located(
            (By.XPATH, '//*[@test_id="win_swReportEndUserWindow_pnl_Katalog_otchetov"]')
        )
    )

    # Мои отчета
    WebDriverWait(browser, 30).until(
        EC.visibility_of_element_located(
            (
                By.XPATH,
                "/html/body/div[1]/div/div[2]/div[5]/div[2]/div[1]/div/div/div/div[2]/div[2]/div[2]/ul/div/li[5]/div/img[1]",
            )
        )
    )
    element = browser.find_element(
        By.XPATH,
        "/html/body/div[1]/div/div[2]/div[5]/div[2]/div[1]/div/div/div/div[2]/div[2]/div[2]/ul/div/li[5]/div/img[1]",
    )
    element.click()
    # Отчет по приемным
    WebDriverWait(browser, 30).until(
        EC.visibility_of_element_located(
            (
                By.XPATH,
                "/html/body/div[1]/div/div[2]/div[5]/div[2]/div[1]/div/div/div/div[2]/div[2]/div[2]/ul/div/li[5]/ul/li/div/a",
            )
        )
    )
    element = browser.find_element(
        By.XPATH,
        "/html/body/div[1]/div/div[2]/div[5]/div[2]/div[1]/div/div/div/div[2]/div[2]/div[2]/ul/div/li[5]/ul/li/div/a",
    )
    element.click()
    # Дата начала = вчера
    WebDriverWait(browser, 30).until(
        EC.element_to_be_clickable(
            (
                By.XPATH,
                '//*[@test_id="win_swReportEndUserWindow_pnl_Otchet_-_Spisok_patsientov,_postupivshih_v_priёmnoe_otdelenie_undefined_paramBegDate"]',
            )
        )
    )
    element = browser.find_element(
        By.XPATH,
        '//*[@test_id="win_swReportEndUserWindow_pnl_Otchet_-_Spisok_patsientov,_postupivshih_v_priёmnoe_otdelenie_undefined_paramBegDate"]',
    )
    actions.click(element).key_down(Keys.CONTROL).send_keys("a").key_up(
        Keys.CONTROL
    ).send_keys(config.yesterday_date.strftime("%d.%m.%Y")).send_keys(
        Keys.ENTER
    ).perform()
    browser.implicitly_wait(15)
    # Формат
    element = browser.find_element(
        By.XPATH, '//*[@test_id="win_swReportEndUserWindow_tbr_undefined"]'
    )
    element.click()
    # XLSX
    element = browser.find_element(By.XPATH, '//div[text()="Формат XLSX"]')
    element.click()
    # Сформировать отчет
    WebDriverWait(browser, 30).until(
        EC.element_to_be_clickable(
            (
                By.XPATH,
                '//*[@test_id="win_swReportEndUserWindow_btn_Sformirovat_otchet"]',
            )
        )
    )
    element = browser.find_element(
        By.XPATH, '//*[@test_id="win_swReportEndUserWindow_btn_Sformirovat_otchet"]'
    )
    element.click()
    config.browser.save_screenshot(os.path.join(reports_path, "save file1.png"))
    # Проверка, что открылось окно скачивания отчета
    WebDriverWait(browser, 20).until(EC.number_of_windows_to_be(2))
    logging.info(f"Начинается сохранение файла с отчетом в папку: {reports_path}")
    utils.download_wait(reports_path, 300, len(os.listdir(reports_path)) + 1)
    config.browser.save_screenshot(os.path.join(reports_path, "save file2.png"))
    logging.info("Сохранение файла с отчетом завершено")
