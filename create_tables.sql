-- Создание таблицы отзывов
CREATE TABLE IF NOT EXISTS reviews (
    id SERIAL PRIMARY KEY,
    author VARCHAR(100),
    review_date DATE NOT NULL,
    rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
    content TEXT NOT NULL,
    source VARCHAR(50) DEFAULT 'vl.ru',
    review_hash VARCHAR(64) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для оптимизации запросов
CREATE INDEX IF NOT EXISTS idx_reviews_date ON reviews(review_date);
CREATE INDEX IF NOT EXISTS idx_reviews_rating ON reviews(rating);
CREATE INDEX IF NOT EXISTS idx_reviews_hash ON reviews(review_hash);

-- Комментарии к таблице и полям
COMMENT ON TABLE reviews IS 'Таблица для хранения отзывов о ВГУЭС';
COMMENT ON COLUMN reviews.author IS 'Автор отзыва';
COMMENT ON COLUMN reviews.review_date IS 'Дата публикации отзыва';
COMMENT ON COLUMN reviews.rating IS 'Оценка (1-5 звезд)';
COMMENT ON COLUMN reviews.content IS 'Текст отзыва';
COMMENT ON COLUMN reviews.source IS 'Источник отзыва';
COMMENT ON COLUMN reviews.review_hash IS 'Уникальный хэш отзыва для предотвращения дубликатов';
