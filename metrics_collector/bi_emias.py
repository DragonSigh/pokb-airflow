import metrics_collector.config as config
import metrics_collector.utils as utils
import os

import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

browser = config.browser
actions = config.actions
reports_path = config.reports_path


def authorize(login_data: str, password_data: str):
    logging.info("Начата авторизация, логин: " + login_data)
    # Очистить куки
    browser.delete_all_cookies()
    # Убедиться что открыта только одна вкладка
    if len(browser.window_handles) > 1:
        browser.switch_to.window(browser.window_handles[1])
        browser.close()
        browser.switch_to.window(browser.window_handles[0])
    # Страница авторизации
    browser.get("http://bi.mz.mosreg.ru/login/")
    # Ввести логин
    login_field = browser.find_element(By.XPATH, '//*[@id="login"]')
    actions.click(login_field).key_down(Keys.CONTROL).send_keys("a").key_up(
        Keys.CONTROL
    ).send_keys(login_data).perform()
    # Ввести пароль
    password_field = browser.find_element(By.XPATH, '//*[@id="password"]')
    actions.click(password_field).key_down(Keys.CONTROL).send_keys("a").key_up(
        Keys.CONTROL
    ).send_keys(password_data).send_keys(Keys.ENTER).perform()

    WebDriverWait(browser, 60).until(
        EC.invisibility_of_element(
            (By.XPATH, "//div[@data-componentid='ext-progress-1']")
        )
    )

    logging.info("Авторизация пройдена")


def load_any_report(
    report_name, use_dates=True, begin_date=config.first_date, end_date=config.last_date
):
    logging.info(f"Открываю страницу отчета {report_name}")

    browser.get("http://bi.mz.mosreg.ru/#form/" + report_name)

    if use_dates:
        logging.info(
            f"Выбран период:"
            f" с {begin_date.strftime('%d.%m.%Y')}"
            f" по {end_date.strftime('%d.%m.%Y')}"
        )

        WebDriverWait(browser, 30).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//input[@name='BegDate']")
            )
        )

        WebDriverWait(browser, 60).until(
            EC.invisibility_of_element(
                (By.XPATH, '//div[@data-componentid="ext-toolbar-8"]')
            )
        )

        # Если строится отчет pass_dvn
        if report_name == "pass_dvn":
            # Фильтр ОГРН
            #browser.execute_script(
            #    "var ogrn_filter = globalThis.Ext.getCmp('ext-RTA-grid-textfilter-24'); +\
            #               ogrn_filter.setValue('1215000036305'); + \
            #               ogrn_filter.fireEvent('select');"
            #)
            browser.save_screenshot(
                os.path.join(reports_path, "test.png")
            )
            browser.execute_script(
                "var first_date = globalThis.Ext.getCmp('ext-datefield-5'); +\
                                first_date.setValue('"
                + begin_date.strftime("%d.%m.%Y")
                + "'); + \
                                first_date.fireEvent('select');"
            )
            browser.execute_script(
                "var last_date = globalThis.Ext.getCmp('ext-datefield-6'); +\
                                last_date.setValue('"
                + end_date.strftime("%d.%m.%Y")
                + "'); + \
                                last_date.fireEvent('select');"
            )

        WebDriverWait(browser, 300).until(
            EC.invisibility_of_element(
                (By.XPATH, '//div[@data-componentid="ext-toolbar-8"]')
            )
        )

        browser.find_element(
            By.XPATH, "//button[@data-componentid='ext-button-13']"
        ).click()

        WebDriverWait(browser, 300).until(
            EC.invisibility_of_element(
                (By.XPATH, '//div[@data-componentid="ext-toolbar-8"]')
            )
        )
    else:
        WebDriverWait(browser, 60).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//div[@data-componentid='ext-RTA-gridview-1']")
            )
        )


def export_report():
    logging.info(f"Начинается сохранение файла с отчетом в папку: {reports_path}")
    try:
        os.mkdir(reports_path)
    except FileExistsError:
        pass
    # Нажимаем на кнопку "Выгрузить в Excel" и ожидаем загрузку файла
    browser.find_element(
        By.XPATH, "//button[@data-componentid='ext-button-14']"
    ).click()
    utils.download_wait(reports_path, 600, len(os.listdir(reports_path)) + 1)
    browser.find_element(
        By.XPATH,
        "/html/body/div[1]/div[2]/div/div/div/div[2]/div/div/div[1]/div[1]/div[2]/div/div[3]/div[4]",
    ).click()
    logging.info("Сохранение файла с отчетом завершено")
