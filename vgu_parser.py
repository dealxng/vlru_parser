import os
import re
import time
import logging
import configparser
import hashlib
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("vgu_parser.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('VGU_Parser')


class DatabaseManager:
    """Менеджер для работы с базой данных PostgreSQL"""

    def __init__(self, db_config):
        self.config = db_config
        self.conn = None

    def connect(self):
        """Установка соединения с базой данных"""
        try:
            self.conn = psycopg2.connect(
                dbname=self.config['dbname'],
                user=self.config['user'],
                password=self.config['password'],
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', '5432')
            )
            logger.info("Успешное подключение к PostgreSQL")
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {str(e)}")
            return False

    def disconnect(self):
        """Закрытие соединения с базой данных"""
        if self.conn:
            self.conn.close()
            logger.info("Соединение с PostgreSQL закрыто")

    def save_reviews(self, reviews):
        """Сохранение отзывов в базу данных"""
        if not reviews:
            logger.warning("Нет данных для сохранения в БД")
            return

        if not self.connect():
            return

        inserted = 0
        duplicates = 0
        errors = 0

        try:
            with self.conn.cursor() as cursor:
                # Подготовка данных
                data = [(
                    r['author'],
                    r['date'],
                    r['rating'],
                    r['text'],
                    r['hash']
                ) for r in reviews]

                # Пакетная вставка
                execute_values(
                    cursor,
                    """
                    INSERT INTO reviews (author, review_date, rating, content, review_hash)
                    VALUES %s
                    ON CONFLICT (review_hash) DO NOTHING
                    """,
                    data,
                    page_size=100
                )

                inserted = cursor.rowcount
                duplicates = len(reviews) - inserted

            self.conn.commit()
            logger.info(f"Сохранено в БД: {inserted} новых, {duplicates} дубликатов")

        except psycopg2.Error as e:
            errors = len(reviews)
            logger.error(f"Ошибка при сохранении в БД: {str(e)}")
            self.conn.rollback()
        finally:
            self.disconnect()

        return {
            'inserted': inserted,
            'duplicates': duplicates,
            'errors': errors
        }


class VGUReviewParser:
    """Парсер отзывов о ВГУЭС с сайта vl.ru"""

    def __init__(self, config_path='config/config.ini'):
        self.config = self.load_config(config_path)
        self.driver = self.setup_driver()
        self.db_manager = DatabaseManager(self.config['Database'])

    def load_config(self, config_path):
        """Загрузка конфигурации из файла"""
        config = configparser.ConfigParser()
        try:
            config.read(config_path)
            logger.info(f"Конфигурация загружена из {config_path}")
            return config
        except Exception as e:
            logger.error(f"Ошибка загрузки конфигурации: {str(e)}")
            # Возвращаем конфиг по умолчанию
            return self.get_default_config()

    def get_default_config(self):
        """Конфигурация по умолчанию"""
        config = configparser.ConfigParser()

        # Настройки базы данных
        config['Database'] = {
            'dbname': 'reviews_db',
            'user': 'postgres',
            'password': 'your_password',
            'host': 'localhost',
            'port': '5432'
        }

        # Настройки Selenium
        config['Selenium'] = {
            'headless': 'True',
            'window_size': '1920,1080',
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'use_webdriver_manager': 'True'
        }

        # Настройки парсера
        config['Website'] = {
            'url': 'https://www.vl.ru/vgues-vladivostoxkij-gosudarstvennyj-universitet'
        }

        config['Parser'] = {
            'max_reviews': '10'
        }

        config['Output'] = {
            'directory': 'data/output'
        }

        return config

    def setup_driver(self):
        """Настройка веб-драйвера для Selenium"""
        chrome_options = Options()

        # Конфигурация опций из файла
        if self.config.getboolean('Selenium', 'headless', fallback=True):
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(f"--window-size={self.config.get('Selenium', 'window_size', fallback='1920,1080')}")
        chrome_options.add_argument(
            f"user-agent={self.config.get('Selenium', 'user_agent', fallback='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')}")

        # Настройка драйвера
        try:
            if self.config.getboolean('Selenium', 'use_webdriver_manager', fallback=True):
                driver = webdriver.Chrome(
                    service=Service(ChromeDriverManager().install()),
                    options=chrome_options
                )
            else:
                driver_path = self.config.get('Selenium', 'driver_path', fallback='')
                service = Service(driver_path) if driver_path else None
                driver = webdriver.Chrome(service=service, options=chrome_options)

            logger.info("Веб-драйвер успешно инициализирован")
            return driver
        except Exception as e:
            logger.error(f"Ошибка инициализации веб-драйвера: {str(e)}")
            # Попробуем ручную настройку
            try:
                driver = webdriver.Chrome(options=chrome_options)
                return driver
            except Exception as e:
                logger.critical(f"Не удалось инициализировать веб-драйвер: {str(e)}")
                raise

    def parse_reviews(self, max_reviews=10):
        """Парсинг отзывов с сайта"""
        logger.info("Запуск процесса парсинга отзывов")
        url = self.config.get('Website', 'url',
                              fallback='https://www.vl.ru/vgues-vladivostoxkij-gosudarstvennyj-universitet')

        try:
            logger.info(f"Загрузка страницы: {url}")
            self.driver.get(url)

            # Ожидание загрузки контента
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.comments-list, li[data-type]"))
            )
            logger.info("Основной контент страницы загружен")

            # Прокрутка для загрузки динамического контента
            logger.info("Выполнение прокрутки для загрузки отзывов")
            for _ in range(3):
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)

            # Парсинг HTML
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            review_blocks = soup.select('li[data-type="review"]')
            logger.info(f"Найдено блоков отзывов: {len(review_blocks)}")

            if not review_blocks:
                logger.warning("Отзывы не обнаружены. Возможно изменилась структура сайта.")
                return []

            # Обработка отзывов
            reviews = []
            for i, block in enumerate(review_blocks[:max_reviews]):
                try:
                    review_data = self.parse_review_block(block)
                    if review_data:
                        reviews.append(review_data)
                        logger.info(f"Отзыв #{i + 1} обработан: {review_data['author']}, {review_data['date']}")
                except Exception as e:
                    logger.error(f"Ошибка обработки отзыва #{i + 1}: {str(e)}")

            logger.info(f"Успешно обработано отзывов: {len(reviews)}/{max_reviews}")
            return reviews

        except Exception as e:
            logger.error(f"Критическая ошибка при парсинге: {str(e)}")
            return []

    def parse_review_block(self, block):
        """Извлечение данных из блока отзыва"""
        # Дата публикации
        timestamp = block.get('data-timestamp')
        date = datetime.utcfromtimestamp(int(timestamp)).date() if timestamp else None

        # Рейтинг
        rating_value = block.get('user-rating', '0')
        try:
            rating_value = float(rating_value)
            # Конвертация в 5-балльную систему с защитой от некорректных значений
            rating = max(1, min(5, round(rating_value * 5))) if rating_value > 0 else 1
        except ValueError:
            rating = 1

        # Автор
        author_elem = block.select_one('span.user-name, .cmt-user-name span')
        author = author_elem.text.strip() if author_elem else "Аноним"

        # Текст отзыва
        text_elem = block.select_one('p.comment-text, .comment-text')
        text = text_elem.text.strip() if text_elem else ""
        text = re.sub(r'\*{3,}', '', text)  # Удаление цензуры
        text = re.sub(r'\s+', ' ', text)  # Нормализация пробелов

        # Уникальный хэш
        review_hash = hashlib.sha256(f"{author}{timestamp}{text}".encode()).hexdigest()

        return {
            'author': author,
            'date': date,
            'rating': rating,
            'text': text,
            'hash': review_hash
        }

    def save_to_files(self, reviews, output_dir='output'):
        """Экспорт данных в файлы"""
        if not reviews:
            logger.warning("Нет данных для экспорта")
            return

        try:
            os.makedirs(output_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # CSV
            csv_path = os.path.join(output_dir, f'vgu_reviews_{timestamp}.csv')
            pd.DataFrame(reviews).to_csv(csv_path, index=False, encoding='utf-8-sig')
            logger.info(f"Данные экспортированы в CSV: {csv_path}")

            # Excel
            excel_path = os.path.join(output_dir, f'vgu_reviews_{timestamp}.xlsx')
            pd.DataFrame(reviews).to_excel(excel_path, index=False)
            logger.info(f"Данные экспортированы в Excel: {excel_path}")

        except Exception as e:
            logger.error(f"Ошибка экспорта данных: {str(e)}")

    def run(self):
        """Основной рабочий процесс"""
        try:
            logger.info("=" * 50)
            logger.info("ЗАПУСК ПАРСЕРА ОТЗЫВОВ ВГУЭС")
            logger.info("=" * 50)

            # Парсинг данных
            reviews = self.parse_reviews(
                max_reviews=self.config.getint('Parser', 'max_reviews', fallback=10)
            )

            if reviews:
                # Экспорт в файлы
                output_dir = self.config.get('Output', 'directory', fallback='data/output')
                self.save_to_files(reviews, output_dir)

                # Сохранение в БД
                self.db_manager.save_reviews(reviews)

            logger.info("ПРОЦЕСС ЗАВЕРШЕН УСПЕШНО")
            return True

        except Exception as e:
            logger.error(f"КРИТИЧЕСКАЯ ОШИБКА: {str(e)}")
            return False
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("Веб-драйвер остановлен")


if __name__ == "__main__":
    # Определение пути к конфигурационному файлу
    config_path = 'config/config.ini'
    if not os.path.exists(config_path):
        logger.warning(f"Конфигурационный файл {config_path} не найден. Будет использована конфигурация по умолчанию.")

    parser = VGUReviewParser(config_path)
    parser.run()
