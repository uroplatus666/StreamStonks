-- 1. Таблица-справочник акций (Сектора, Названия)
CREATE TABLE IF NOT EXISTS tickers (
    symbol VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(150),
    sector VARCHAR(100)
);

-- 2. Основная таблица с данными (Свечи)
CREATE TABLE IF NOT EXISTS market_candles (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    open NUMERIC(12, 6),
    high NUMERIC(12, 6),
    low  NUMERIC(12, 6),
    close NUMERIC(12, 6),
    volume BIGINT,
    macd NUMERIC(12,6),
    rsi NUMERIC(12,6),
    day_of_week INT,
    time_of_day INT,
    
    -- Связь с таблицей tickers (если удалим тикер, удалятся и свечи)
    CONSTRAINT fk_ticker FOREIGN KEY (ticker) REFERENCES tickers(symbol) ON DELETE CASCADE,
    -- Защита от дублей
    CONSTRAINT uniq_ticker_ts UNIQUE (ticker, ts)
);

-- Индекс для ускорения поиска
CREATE INDEX IF NOT EXISTS idx_candles_ticker_ts
ON market_candles(ticker, ts);

-- 3. Таблица для предсказаний модели (на будущее)
CREATE TABLE IF NOT EXISTS model_predictions (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10),
    ts TIMESTAMPTZ,
    horizon INTERVAL,
    action VARCHAR(10), -- 'BUY', 'SELL', 'HOLD'
    score NUMERIC(10,5),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 4. История портфеля (для симуляции торговли)
CREATE TABLE IF NOT EXISTS portfolio_history (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMPTZ,
    ticker VARCHAR(10),
    price NUMERIC(12, 6),
    position INT,
    portfolio_value NUMERIC(14, 6)
);


-- Таблица логов для Grafana (метрики качества и нагрузки)
CREATE TABLE IF NOT EXISTS prediction_logs (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10),
    requested_at TIMESTAMPTZ DEFAULT NOW(),
    horizon_hours INT,
    processing_time_ms INT,
    predicted_start_price NUMERIC(12, 6), -- Цена на момент прогноза
    predicted_end_price NUMERIC(12, 6)    -- Цена в конце горизонта
);