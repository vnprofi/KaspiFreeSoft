import re
import time
import threading
import webbrowser
import sys
import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                             QHBoxLayout, QLabel, QLineEdit, QPushButton,
                             QTextEdit, QRadioButton, QButtonGroup, QProgressBar,
                             QFrame, QMessageBox, QFileDialog)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import QFont, QPalette, QColor

LANG = {
    'ru': {
        'title': "ParserKaspiFree v 1.0",
        'url_label': "Введите URL каталога:",
        'start': "Начать парсинг",
        'stop': "Остановить парсинг",
        'save_as': "Формат сохранения:",
        'log_label': "Логирование:",
        'contact': "Связаться",
        'error_url': "Пожалуйста, введите корректный URL!",
        'done_msg': "Парсинг завершён. Данные сохранены.",
        'stopped_msg': "Парсинг остановлен пользователем.",
    }
}

current_lang = 'ru'


def parse_product_details(driver, link, log_signal):
    specifications_list, seller_columns, price_columns = [], [], []
    additional_dict = {}
    try:
        driver.execute_script("window.open('', '_blank');")
        driver.switch_to.window(driver.window_handles[-1])
        driver.get(link)
        time.sleep(1)
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        for element in soup.find_all('ul', class_='short-specifications'):
            for spec in element.find_all('li', class_='short-specifications__text'):
                specifications_list.append(spec.text.strip())

        try:
            next_button = driver.find_elements(By.XPATH,
                                               '//li[contains(@class, "tabs-content__tab") and contains(text(), "Характеристики")]')
            if next_button:
                driver.execute_script("arguments[0].click();", next_button[0])
                time.sleep(1)
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                for el in soup.find_all('dl', class_='specifications-list__el'):
                    for spec in el.find_all('dl', class_='specifications-list__spec'):
                        term = spec.find('span', class_='specifications-list__spec-term-text')
                        val = spec.find('dd', class_='specifications-list__spec-definition')
                        if term and val:
                            additional_dict[term.text.strip()] = val.text.strip()
        except:
            pass

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        sellers_table = soup.find('table', class_='sellers-table__self')
        unique_sellers = set()
        if sellers_table:
            for row in sellers_table.find_all('tr'):
                link_el = row.find('a', href=True)
                if link_el:
                    name = link_el.text.strip()
                    price_el = row.find('div', class_='sellers-table__price-cell-text')
                    price = re.sub(r'\s+', ' ', price_el.text.replace('\xa0', '')) if price_el else None
                    if name not in unique_sellers:
                        seller_columns.append(name)
                        price_columns.append(price)
                        unique_sellers.add(name)

        specifications_dict = {}
        for spec in specifications_list:
            if ':' in spec:
                k, v = spec.split(':', 1)
                specifications_dict[k.strip()] = v.strip()

        driver.close()
        driver.switch_to.window(driver.window_handles[0])

        result = {**specifications_dict, **additional_dict}
        for i in range(min(len(seller_columns), len(price_columns), 6)):
            result[f"Seller_{i + 1}"] = seller_columns[i]
            result[f"Price_{i + 1}"] = price_columns[i]
        return result

    except Exception as e:
        log_signal.emit(f"Ошибка парсинга товара: {e}")
        try:
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
        except:
            pass
        return {}


class ScraperThread(QThread):
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(bool)
    finished_signal = pyqtSignal()
    data_ready = pyqtSignal(object, str)

    def __init__(self, url, format_type):
        super().__init__()
        self.url = url
        self.format_type = format_type
        self.stop_parsing = False

    def stop(self):
        self.stop_parsing = True
        self.log_signal.emit("Получен сигнал остановки парсинга...")

    def run(self):
        self.progress_signal.emit(True)

        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-plugins")
        options.add_argument("--disable-web-security")
        options.add_argument("--allow-running-insecure-content")
        options.add_experimental_option('useAutomationExtension', False)
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver.implicitly_wait(3)
        driver.set_page_load_timeout(15)

        data = []

        try:
            driver.get(self.url)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "item-card__info"))
            )
        except Exception as e:
            self.log_signal.emit(f"Ошибка загрузки: {e}")
            self.progress_signal.emit(False)
            driver.quit()
            self.finished_signal.emit()
            return

        page_num = 1

        while not self.stop_parsing:
            try:
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "item-card__info"))
                )

                soup = BeautifulSoup(driver.page_source, 'html.parser')
                products = soup.find_all(class_='item-card__info')

                if not products:
                    self.log_signal.emit("Товары не найдены на текущей странице")
                    break

                for product in products:
                    if self.stop_parsing:
                        break
                    try:
                        title = product.find(class_='item-card__name').text.strip()
                        rel_link = product.find('a', class_='item-card__name-link')['href']
                        link = f"https://kaspi.kz{rel_link}"
                        price = product.find('span', class_='item-card__prices-price').text.strip()
                        rating_el = product.find(class_='item-card__rating')
                        rating = rating_el.text.strip() if rating_el else 'Нет рейтинга'

                        details = parse_product_details(driver, link, self.log_signal)
                        product_data = {
                            'Название': title,
                            'Ссылка': link,
                            'Цена': price,
                            'Рейтинг': rating,
                            **details,
                        }
                        data.append(product_data)
                        self.log_signal.emit(f"Собран товар: {title}")

                    except Exception as e:
                        self.log_signal.emit(f"Ошибка: {e}")

                if self.stop_parsing:
                    self.log_signal.emit("Остановка парсинга после текущей страницы...")
                    break

                try:
                    next_button = driver.find_elements(By.XPATH,
                                                       '//li[contains(@class, "pagination__el") and contains(text(), "Следующая")]')
                    if next_button and 'disabled' not in next_button[0].get_attribute('class'):
                        driver.execute_script("arguments[0].click();", next_button[0])
                        page_num += 1
                        self.log_signal.emit(f"Переход на страницу {page_num}")
                        time.sleep(2)
                    else:
                        self.log_signal.emit("Достигнута последняя страница.")
                        break
                except Exception:
                    break

            except Exception as e:
                self.log_signal.emit(f"Ошибка на странице {page_num}: {e}")
                break

        self.progress_signal.emit(False)

        if data:
            df = pd.DataFrame(data)
            self.log_signal.emit(f"Собрано товаров: {len(data)}")
            self.data_ready.emit(df, self.format_type)
        else:
            self.log_signal.emit("Нет данных для сохранения.")

        if self.stop_parsing:
            self.log_signal.emit(LANG[current_lang]['stopped_msg'])

        try:
            driver.quit()
        except Exception as e:
            self.log_signal.emit(f"Ошибка при закрытии драйвера: {e}")

        self.finished_signal.emit()


class ModernButton(QPushButton):
    def __init__(self, text, primary=False):
        super().__init__(text)
        if primary:
            self.setStyleSheet("""
                QPushButton {
                    background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                                stop: 0 #4CAF50, stop: 1 #45a049);
                    border: none;
                    color: white;
                    padding: 12px 24px;
                    font-size: 14px;
                    font-weight: bold;
                    border-radius: 8px;
                    min-width: 120px;
                }
                QPushButton:hover {
                    background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                                stop: 0 #5CBDB0, stop: 1 #4CAF50);
                    transform: translateY(-1px);
                }
                QPushButton:pressed {
                    background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                                stop: 0 #45a049, stop: 1 #3d8b40);
                }
                QPushButton:disabled {
                    background: #cccccc;
                    color: #666666;
                }
            """)
        else:
            self.setStyleSheet("""
                QPushButton {
                    background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                                stop: 0 #f44336, stop: 1 #d32f2f);
                    border: none;
                    color: white;
                    padding: 12px 24px;
                    font-size: 14px;
                    font-weight: bold;
                    border-radius: 8px;
                    min-width: 120px;
                }
                QPushButton:hover {
                    background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                                stop: 0 #f66356, stop: 1 #f44336);
                }
                QPushButton:pressed {
                    background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                                stop: 0 #d32f2f, stop: 1 #b71c1c);
                }
                QPushButton:disabled {
                    background: #cccccc;
                    color: #666666;
                }
            """)


class ContactButton(QPushButton):
    def __init__(self, text):
        super().__init__(text)
        self.setStyleSheet("""
            QPushButton {
                background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                            stop: 0 #2196F3, stop: 1 #1976D2);
                border: none;
                color: white;
                padding: 10px 20px;
                font-size: 12px;
                font-weight: bold;
                border-radius: 20px;
                min-width: 100px;
            }
            QPushButton:hover {
                background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                            stop: 0 #42A5F5, stop: 1 #2196F3);
                transform: translateY(-1px);
            }
            QPushButton:pressed {
                background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                            stop: 0 #1976D2, stop: 1 #1565C0);
            }
        """)


class KaspiParser(QMainWindow):
    def __init__(self):
        super().__init__()
        self.scraper_thread = None
        self.init_ui()

    def init_ui(self):
        self.setWindowTitle(LANG[current_lang]['title'])
        self.setGeometry(100, 100, 1000, 750)

        # Устанавливаем темную тему
        self.setStyleSheet("""
            QMainWindow {
                background-color: #2b2b2b;
                color: #ffffff;
            }
            QWidget {
                background-color: #2b2b2b;
                color: #ffffff;
            }
            QLabel {
                color: #ffffff;
                font-size: 13px;
            }
            QLineEdit {
                background-color: #3c3c3c;
                border: 2px solid #555555;
                border-radius: 8px;
                padding: 12px;
                font-size: 13px;
                color: #ffffff;
            }
            QLineEdit:focus {
                border-color: #4CAF50;
                background-color: #404040;
            }
            QTextEdit {
                background-color: #1e1e1e;
                border: 2px solid #555555;
                border-radius: 8px;
                padding: 8px;
                font-family: 'Consolas', monospace;
                font-size: 12px;
                color: #ffffff;
            }
            QRadioButton {
                color: #ffffff;
                font-size: 12px;
                spacing: 5px;
            }
            QRadioButton::indicator {
                width: 18px;
                height: 18px;
            }
            QRadioButton::indicator:unchecked {
                border: 2px solid #555555;
                border-radius: 9px;
                background-color: #3c3c3c;
            }
            QRadioButton::indicator:checked {
                border: 2px solid #4CAF50;
                border-radius: 9px;
                background-color: #4CAF50;
            }
            QProgressBar {
                border: 2px solid #555555;
                border-radius: 8px;
                background-color: #3c3c3c;
                text-align: center;
                color: #ffffff;
                font-weight: bold;
            }
            QProgressBar::chunk {
                background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                                  stop: 0 #4CAF50, stop: 1 #45a049);
                border-radius: 6px;
            }
        """)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        layout = QVBoxLayout(central_widget)
        layout.setSpacing(20)
        layout.setContentsMargins(30, 30, 30, 30)

        # Заголовок с кнопкой связи
        header_layout = QHBoxLayout()
        title_label = QLabel(LANG[current_lang]['title'])
        title_label.setFont(QFont("Segoe UI", 24, QFont.Weight.Bold))
        title_label.setStyleSheet("color: #4CAF50; margin-bottom: 10px;")
        header_layout.addWidget(title_label)
        header_layout.addStretch()

        # Кнопка связи
        contact_btn = ContactButton(LANG[current_lang]['contact'])
        contact_btn.clicked.connect(self.open_contact)
        header_layout.addWidget(contact_btn)

        layout.addLayout(header_layout)

        # URL input
        url_label = QLabel(LANG[current_lang]['url_label'])
        url_label.setFont(QFont("Segoe UI", 14, QFont.Weight.Bold))
        layout.addWidget(url_label)

        self.url_entry = QLineEdit()
        self.url_entry.setText(
            "https://kaspi.kz/shop/c/tv_audio/?q=%3Acategory%3ATV_Audio%3AavailableInZones%3AMagnum_ZONE1&sort=relevance&sc=")
        self.url_entry.setFont(QFont("Segoe UI", 12))
        layout.addWidget(self.url_entry)

        # Формат сохранения
        format_label = QLabel(LANG[current_lang]['save_as'])
        format_label.setFont(QFont("Segoe UI", 14, QFont.Weight.Bold))
        layout.addWidget(format_label)

        format_layout = QHBoxLayout()
        self.format_group = QButtonGroup()

        for i, fmt in enumerate(['xlsx', 'csv', 'json']):
            radio = QRadioButton(fmt.upper())
            radio.setFont(QFont("Segoe UI", 12))
            self.format_group.addButton(radio, i)
            format_layout.addWidget(radio)
            if fmt == 'xlsx':
                radio.setChecked(True)

        format_layout.addStretch()
        layout.addLayout(format_layout)

        # Кнопки управления
        button_layout = QHBoxLayout()

        self.start_btn = ModernButton(LANG[current_lang]['start'], primary=True)
        self.start_btn.clicked.connect(self.start_parsing)
        button_layout.addWidget(self.start_btn)

        self.stop_btn = ModernButton(LANG[current_lang]['stop'])
        self.stop_btn.clicked.connect(self.stop_parsing)
        self.stop_btn.setEnabled(False)
        button_layout.addWidget(self.stop_btn)

        button_layout.addStretch()
        layout.addLayout(button_layout)

        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)  # Indeterminate progress
        self.progress_bar.hide()
        layout.addWidget(self.progress_bar)

        # Лог
        log_label = QLabel(LANG[current_lang]['log_label'])
        log_label.setFont(QFont("Segoe UI", 14, QFont.Weight.Bold))
        layout.addWidget(log_label)

        self.log_text = QTextEdit()
        self.log_text.setFont(QFont("Consolas", 11))
        self.log_text.setReadOnly(True)
        layout.addWidget(self.log_text)

    def open_contact(self):
        webbrowser.open('https://t.me/Userspoi')

    def log_message(self, message):
        timestamp = time.strftime("%H:%M:%S")
        self.log_text.append(f"[{timestamp}] {message}")

    def get_selected_format(self):
        formats = ['xlsx', 'csv', 'json']
        return formats[self.format_group.checkedId()]

    def start_parsing(self):
        url = self.url_entry.text().strip()
        if not url.startswith('http'):
            QMessageBox.warning(self, "Ошибка", LANG[current_lang]['error_url'])
            return

        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)

        format_type = self.get_selected_format()

        self.scraper_thread = ScraperThread(url, format_type)
        self.scraper_thread.log_signal.connect(self.log_message)
        self.scraper_thread.progress_signal.connect(self.toggle_progress)
        self.scraper_thread.finished_signal.connect(self.parsing_finished)
        self.scraper_thread.data_ready.connect(self.save_data)
        self.scraper_thread.start()

    def stop_parsing(self):
        if self.scraper_thread and self.scraper_thread.isRunning():
            self.log_message("Остановка парсинга...")
            self.scraper_thread.stop()
        else:
            self.log_message("Парсинг не запущен")

    def toggle_progress(self, show):
        if show:
            self.progress_bar.show()
        else:
            self.progress_bar.hide()

    def save_data(self, df, format_type):
        """Сохранение данных через диалог выбора файла (как в исходном коде)"""
        try:
            # Определяем фильтры файлов
            if format_type == 'xlsx':
                file_filter = "Excel files (*.xlsx)"
                default_name = "kaspi_data.xlsx"
            elif format_type == 'csv':
                file_filter = "CSV files (*.csv)"
                default_name = "kaspi_data.csv"
            elif format_type == 'json':
                file_filter = "JSON files (*.json)"
                default_name = "kaspi_data.json"

            file_path, _ = QFileDialog.getSaveFileName(
                self,
                "Сохранить файл",
                default_name,
                file_filter
            )

            if file_path:
                self.log_message("Сохранение файла...")
                if format_type == 'xlsx':
                    df.to_excel(file_path, index=False)
                elif format_type == 'csv':
                    df.to_csv(file_path, index=False)
                elif format_type == 'json':
                    df.to_json(file_path, orient='records', force_ascii=False)

                self.log_message(LANG[current_lang]['done_msg'])
                self.log_message(f"Файл сохранен: {file_path}")
            else:
                self.log_message("Сохранение отменено пользователем")

        except Exception as e:
            self.log_message(f"Ошибка сохранения: {e}")
            QMessageBox.critical(self, "Ошибка сохранения", f"Не удалось сохранить файл:\n{str(e)}")

    def parsing_finished(self):
        """Обработка завершения парсинга"""
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.progress_bar.hide()
        self.log_message("Парсинг завершен. Готов к новому запуску.")


def main():
    app = QApplication(sys.argv)

    app.setApplicationName("ParserKaspiFree v 1.0")
    app.setApplicationVersion("1.0")

    window = KaspiParser()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()