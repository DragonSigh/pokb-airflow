import metrics_collector.config as config
import metrics_collector.utils as utils
import logging
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

browser = config.browser
actions = config.actions
reports_path = config.reports_path


def authorize(login_data: str, password_data: str):
    browser.get("http://llo.emias.mosreg.ru/korvet/admin/signin")
    browser.refresh()
    login_field = browser.find_element(
        By.XPATH, '//*[@id="content"]/div/div/form/div[1]/input'
    )
    login_field.send_keys(login_data)
    password_field = browser.find_element(
        By.XPATH, '//*[@id="content"]/div/div/form/div[2]/input'
    )
    password_field.send_keys(password_data)
    browser.find_element(
        By.XPATH, '//*[@id="content"]/div/div/form/div[4]/button'
    ).click()

    WebDriverWait(browser, 60).until(
        EC.presence_of_element_located((By.XPATH, "//*[@id='aspnetForm']/header/nav/ul/li[3]"))
    )

    logging.info("Авторизация пройдена")
    browser.save_screenshot(r'/etc/samba/share/upload/kornet_auth.png')


def load_dlo_report(begin_date, end_date):
    logging.info("Открываю страницу отчёта")
    browser.get(
        "http://llo.emias.mosreg.ru/korvet/LocalReportForm.aspx?"
        "guid=85122D62-3F72-40B5-A7ED-B2AFBF27560B&FundingSource=0&BeginDate="
        + begin_date.strftime("%d.%m.%Y")
        + "&EndDate="
        + end_date.strftime("%d.%m.%Y")
    )
    browser.save_screenshot(r'/etc/samba/share/upload/kornet_rep2.png')
    browser.implicitly_wait(5)
    logging.info("Отчет сформирован в браузере")


def export_report():
    # Создать папку с отчётами, если её нет в системе
    # try:
    #    os.mkdir(reports_path)
    # except FileExistsError:
    #    pass
    # Ожидать загрузки отчёта в веб-интерфейсе
    logging.info("Начинается экспорт отчета")
    WebDriverWait(browser, 30).until(
        EC.element_to_be_clickable(
            (
                By.XPATH,
                "/html/body/form/table/tbody/tr/td/div/span/div/table/tbody/tr[4]/"
                "td[3]/div/div[1]/div/table/tbody/tr/td/table/tbody/tr/td/table/tbody/tr[8]",
            )
        )
    )
    # Выполнить javascript для выгрузки  в Excel, который прописан в кнопке
    browser.execute_script(
        "$find('ctl00_plate_reportViewer').exportReport('EXCELOPENXML');"
    )
    utils.download_wait(config.reports_path, 20)
    logging.info("Экспорт файла с отчетом завершен")
    browser.get("http://llo.emias.mosreg.ru/korvet/Admin/SignOut")
