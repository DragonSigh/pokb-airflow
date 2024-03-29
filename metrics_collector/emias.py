import metrics_collector.config as config
import metrics_collector.utils as utils
import os
import logging
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Получает объект браузера из конфигурации
browser = config.browser

# Получает объект действий с клавиатурой и мышью из конфигурации
actions = config.actions

# Получает путь к папке с отчетами из конфигурации
reports_path = config.reports_path


def authorize(login_data: str, password_data: str):
    """
    Авторизация в ЕМИАС МО
    http://main.emias.mosreg.ru/MIS/Podolsk_GKB
    """
    logging.info("Начинается авторизация в ЕМИАС")
    # Очистить куки
    browser.delete_all_cookies()
    # Убедиться что открыта только одна вкладка
    if len(browser.window_handles) > 1:
        browser.switch_to.window(browser.window_handles[1])
        browser.close()
        browser.switch_to.window(browser.window_handles[0])
    # Открыть страницу ввода логина и пароля
    browser.get("http://main.emias.mosreg.ru/MIS/Podolsk_gkb/")
    # Ввести логин
    login_field = browser.find_element(By.XPATH, '//*[@id="Login"]')
    actions.click(login_field).key_down(Keys.CONTROL).send_keys("a").key_up(
        Keys.CONTROL
    ).send_keys(login_data).perform()
    # Ввести пароль
    password_field = browser.find_element(By.XPATH, '//*[@id="Password"]')
    actions.click(password_field).key_down(Keys.CONTROL).send_keys("a").key_up(
        Keys.CONTROL
    ).send_keys(password_data).perform()
    # Отметить "Запомнить меня"
    browser.find_element(By.XPATH, '//*[@id="Remember"]').click()
    # Нажать на кнопку "Войти"
    browser.find_element(By.XPATH, '//*[@id="loginBtn"]').click()
    browser.get("http://main.emias.mosreg.ru/MIS/Podolsk_gkb/Main/Default")
    WebDriverWait(browser, 30).until(
        EC.invisibility_of_element((By.XPATH, '//*[@id="loadertext"]'))
    )
    logging.info("Авторизация пройдена")


def load_system_report(cabinet_id, begin_date, end_date):
    """
    Функция открывает Отчет по записи на прием v2 в Системных отчетах.

    Args:
        cabinet_id: Идентификатор кабинета.
        begin_date: Начальная дата периода отчета.
        end_date: Конечная дата периода отчета.
    """
    logging.info(f"Открываю Отчет по записи на прием v2, ID кабинета: {cabinet_id}")

    #  Находим элемент "Системные отчеты" на странице и кликаем по нему
    element = browser.find_element(By.XPATH, '//*[@id="Portlet_9"]/div[2]/div[1]/a')
    WebDriverWait(browser, 20).until(EC.element_to_be_clickable(element))
    browser.execute_script("arguments[0].click();", element)
    WebDriverWait(browser, 20).until(EC.number_of_windows_to_be(2))
    browser.switch_to.window(browser.window_handles[1])

    # Ждем, пока исчезнет индикатор загрузки
    WebDriverWait(browser, 20).until(
        EC.invisibility_of_element((By.XPATH, '//*[@id="loadertext"]'))
    )

    # Вводим "v2" в поле фильтра отчетов.
    element = browser.find_element(By.XPATH, '//*[@id="table_filter"]/label/input')
    actions.click(element).send_keys("v2").perform()

    # Кликам по ссылке на Отчет по записи на прием v2
    element = browser.find_element(By.XPATH, '//*[@id="table"]/tbody/tr/td[3]/a')
    element.click()

    # Ждем, пока станет доступна кнопка "Выполнить"
    element = browser.find_element(By.XPATH, '//*[@id="send-request-btn"]')
    WebDriverWait(browser, 20).until(EC.element_to_be_clickable(element))

    # Вводим период
    element = browser.find_element(By.XPATH, '//*[@id="Arguments_0__Value"]')
    browser.execute_script(
        """
        var elem = arguments[0];
        var value = arguments[1];
        elem.value = value;
    """,
        element,
        begin_date.strftime("%d.%m.%Y") + "_" + end_date.strftime("%d.%m.%Y"),
    )

    # Выбираем кабинет по идентификатору
    element = browser.find_element(By.XPATH, '//*[@id="Arguments_2__Value"]')
    browser.execute_script(
        """
        var elem = arguments[0];
        var value = arguments[1];
        elem.value = value;
    """,
        element,
        cabinet_id,
    )

    # Указываем параметр 0 - без отмененных записей
    element = browser.find_element(By.XPATH, '//*[@id="Arguments_3__Value"]')
    browser.execute_script(
        """
        var elem = arguments[0];
        var value = arguments[1];
        elem.value = value;
    """,
        element,
        "0",
    )

    # Жмем кнопку "Выполнить"
    browser.find_element(By.XPATH, '//*[@id="send-request-btn"]').click()
    logging.info("Отчет открыт в браузере")


def export_system_report(cabinet):
    logging.info("Начинается формирование отчета")
    # Создать папку с отчётами, если её нет в системе
    try:
        os.mkdir(reports_path)
    except FileExistsError:
        pass
    # Сохранить в Excel
    # Ожидать появления надписи "Выполнено"
    try:
        WebDriverWait(browser, 300).until(
            EC.text_to_be_present_in_element_value(
                (By.XPATH, "/html/body/div/div[2]/div/div/form[1]/input"), "done"
            )
        )
    except TimeoutException:
        browser.refresh()
    # Нажать на кнопку "Скачать файл"
    element = browser.find_element(By.XPATH, '//*[@id="dlbId"]')
    element.click()
    # Ожидать скачивания в папку
    utils.download_wait(reports_path, 20)
    # Закрыть браузер и переключиться на другое окно
    browser.close()
    browser.switch_to.window(browser.window_handles[0])
    logging.info(f"Файл с отчетом сохранён в папку: {reports_path}")


def load_tm_report(report_id, begin_date, end_date):
    """
    Открыть Отчеты
    """
    logging.info(f"Открываю отчет с ID {report_id}")
    element = browser.find_element(By.XPATH, '//*[@id="Portlet_9"]/div[2]/div[4]/a')
    element.click()
    browser.switch_to.window(browser.window_handles[1])
    WebDriverWait(browser, 20).until(
        EC.invisibility_of_element((By.XPATH, '//*[@id="loadertext"]'))
    )
    browser.get("http://tm.emias.mosreg.ru/report/reports/externalRun/" + report_id)
    element = browser.find_element(By.XPATH, '//*[@formcontrolname="beginDate"]')
    actions.click(element).key_down(Keys.CONTROL).send_keys("a").key_up(
        Keys.CONTROL
    ).send_keys(begin_date).perform()
    element = browser.find_element(By.XPATH, '//*[@formcontrolname="endDate"]')
    actions.click(element).key_down(Keys.CONTROL).send_keys("a").key_up(
        Keys.CONTROL
    ).send_keys(end_date).perform()
    element = browser.find_element(By.XPATH, '//*[@id="mat-input-1"]')
    actions.click(element).key_down(Keys.CONTROL).send_keys("a").key_up(
        Keys.CONTROL
    ).send_keys("0").perform()

    logging.info("Отчет открыт в браузере")
