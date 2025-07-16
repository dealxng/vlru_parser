import re
import time
import pandas as pd
import psycopg2
from datetime import datetime
import hashlib
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import os

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


def setup_driver():
    """Настройка и запуск Chrome WebDriver"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

    try:
        # Используем системный ChromeDriver
        driver = webdriver.Chrome(options=chrome_options)
        return driver
    except Exception as e:
        logger.error(f"Ошибка при создании драйвера: {str(e)}")
        # Попробуем ручную установку
        try:
            driver_path = os.path.join(os.getcwd(), "chromedriver.exe")
            service = Service(driver_path)
            driver = webdriver.Chrome(service=service, options=chrome_options)
            return driver
        except Exception as e:
            logger.error(f"Ошибка при ручной настройке драйвера: {str(e)}")
            raise


def parse_reviews():
    """Парсинг отзывов с использованием Selenium"""
    logger.info("Начало парсинга отзывов с использованием Selenium")
    URL = "https://www.vl.ru/vgues-vladivostoxkij-gosudarstvennyj-universitet"

    driver = None
    try:
        driver = setup_driver()
        logger.info(f"Загрузка страницы: {URL}")
        driver.get(URL)

        # Ожидание загрузки страницы
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.comments-list, li[data-type]"))
        )
        logger.info("Основная страница загружена")

        # Прокрутка страницы
        logger.info("Прокрутка страницы для загрузки отзывов...")
        for _ in range(2):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)

        # Получение HTML
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')

        # Поиск блоков с отзывами
        review_blocks = soup.select('li[data-type="review"]')
        logger.info(f"Найдено блоков отзывов: {len(review_blocks)}")

        if not review_blocks:
            return []

        # Сбор последних 10 отзывов
        reviews = []
        for i, block in enumerate(review_blocks[:10]):
            try:
                # Извлечение данных
                timestamp = block.get('data-timestamp')
                if timestamp:
                    timestamp = int(timestamp)
                    date = datetime.utcfromtimestamp(timestamp).date()
                else:
                    date = None

                rating_value = block.get('user-rating')
                if rating_value:
                    try:
                        rating_value = float(rating_value)
                        if rating_value > 0:
                            rating = min(5, max(1, round(rating_value * 5)))
                        else:
                            rating = 1
                    except ValueError:
                        rating = 1
                else:
                    rating = 1

                # Автор
                author_elem = block.select_one('span.user-name, .cmt-user-name span')
                author = author_elem.text.strip() if author_elem else "Аноним"

                # Текст
                text_elem = block.select_one('p.comment-text, .comment-text')
                text = text_elem.text.strip() if text_elem else ""

                # Очистка текста
                if text:
                    text = re.sub(r'\*{3,}', '', text)
                    text = re.sub(r'\s+', ' ', text)

                # Хэш
                review_hash = hashlib.sha256(f"{author}{timestamp}{text}".encode()).hexdigest()

                reviews.append({
                    'author': author,
                    'date': date,
                    'rating': rating,
                    'text': text,
                    'hash': review_hash
                })

            except Exception as e:
                logger.error(f"Ошибка обработки отзыва #{i + 1}: {str(e)}")
                continue

        return reviews

    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}")
        return []
    finally:
        if driver:
            driver.quit()


def save_to_files(reviews):
    """Сохраняет отзывы в CSV и Excel файлы"""
    if not reviews:
        return

    try:
        timestamp = int(time.time())
        df = pd.DataFrame(reviews)
        df_output = df.drop(columns=['hash'])

        # CSV
        csv_file = f'vgu_reviews_{timestamp}.csv'
        df_output.to_csv(csv_file, index=False, encoding='utf-8-sig')
        logger.info(f"Данные сохранены в {csv_file}")

        # Excel
        excel_file = f'vgu_reviews_{timestamp}.xlsx'
        df_output.to_excel(excel_file, index=False)
        logger.info(f"Данные сохранены в {excel_file}")

    except Exception as e:
        logger.error(f"Ошибка при сохранении файлов: {str(e)}")


def save_to_db(reviews):
    """Сохраняет отзывы в PostgreSQL"""
    if not reviews:
        return

    conn = None
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(
            dbname="reviews_db",
            user="postgres",
            password="5205055",  # Замените на ваш пароль
            host="localhost",
            port="5432"
        )

        inserted_count = 0
        duplicate_count = 0
        error_count = 0

        # Создаем курсор для выполнения SQL-запросов
        with conn.cursor() as cursor:
            for review in reviews:
                try:
                    # Вставка данных в таблицу
                    cursor.execute(
                        """
                        INSERT INTO reviews (author, review_date, rating, content, review_hash)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (review_hash) DO NOTHING
                        """,
                        (
                            review['author'],
                            review['date'],
                            review['rating'],
                            review['text'],
                            review['hash']
                        )
                    )

                    # Подсчет результатов
                    if cursor.rowcount > 0:
                        inserted_count += 1
                    else:
                        duplicate_count += 1

                except psycopg2.Error as e:
                    error_count += 1
                    logger.error(f"Ошибка БД при вставке отзыва: {str(e)}")
                    # Продолжаем обработку следующих отзывов
                    continue

            # Фиксируем изменения в базе данных
            conn.commit()

        logger.info(
            f"Результат сохранения в БД: Успешно: {inserted_count}, Дубликатов: {duplicate_count}, Ошибок: {error_count}")

    except psycopg2.Error as e:
        logger.error(f"Ошибка подключения к PostgreSQL: {str(e)}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info(" ЗАПУСК ПАРСЕРА ОТЗЫВОВ ВВГУ ")
    logger.info("=" * 50)

    logger.info("Этап 1: Парсинг отзывов")
    reviews = parse_reviews()

    if reviews:
        logger.info(f"Успешно собрано отзывов: {len(reviews)}")

        logger.info("Этап 2: Сохранение в файлы")
        save_to_files(reviews)

        logger.info("Этап 3: Сохранение в базу данных")
        save_to_db(reviews)
    else:
        logger.warning("Не удалось собрать отзывы")

    logger.info("=" * 50)
    logger.info(" РАБОТА ПАРСЕРА ЗАВЕРШЕНА ")
    logger.info("=" * 50)
