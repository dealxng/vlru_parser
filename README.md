# Парсер отзывов ВВГУ

Профессиональное решение для сбора отзывов о Владивостокском государственном университете (ВВГУ) с сайта vl.ru, их сохранения в базу данных PostgreSQL и экспорта в форматы CSV/Excel.

## Особенности

- Автоматический сбор последних 10 отзывов
- Сохранение данных в PostgreSQL с защитой от дубликатов
- Экспорт в CSV и Excel форматы
- Обработка динамического контента через Selenium
- Пакетная вставка данных в БД для оптимизации производительности
- Автономный исполняемый файл

## Требования

- Python 3.8+
- PostgreSQL 14+
- Google Chrome (для работы Selenium)
- Установленные зависимости из `requirements.txt`

## Описание работы

- 1. Инициализация драйвера:
    - Настройка headless-режима Chrome
    - Установка параметров браузера




    
##Настройка

- Для подключение к вашей датабазе в коде с 183 по 196 строку представленна функция для подключения
- При необходимости данные в ней нужно заменить
```bash
conn = None
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(
            dbname="reviews_db",
            user="postgres", 
            password="your_password",  # Замените на ваш пароль
            host="localhost",
            port="5432"
        )
```

## Установка

1. Клонируйте репозиторий:
```bash
git clone https://github.com/yourusername/vgu-reviews-parser.git
cd vgu-reviews-parser
```

## Развертывание и использование

- Создайте базу данных:
```bash
psql -U postgres -c "CREATE DATABASE reviews_db;"
psql -U postgres -d reviews_db -f create_table.sql
```
- Настройте подключение к БД в файле `vgu_parser.py`
- Запустите парсер:
```bash
python vgu_parser.py
```
- Проверить результаты:
```psql
-- В psql
\c reviews_db
SELECT * FROM reviews LIMIT 5;
```
