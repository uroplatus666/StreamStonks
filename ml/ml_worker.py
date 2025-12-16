import json
import os
import time
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from catboost import CatBoostRegressor
from kafka import KafkaConsumer
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ML_Worker")

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
PG_HOST = os.getenv("POSTGRES_HOST")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASS = os.getenv("POSTGRES_PASSWORD")
PG_DB = os.getenv("POSTGRES_DB")

model = CatBoostRegressor()
MODEL_PATH = "shared_models/catboost_model.cbm"

# Если папки нет (локальный запуск), ищем файл рядом
if not os.path.exists(MODEL_PATH) and os.path.exists("catboost_model.cbm"):
    MODEL_PATH = "catboost_model.cbm"
last_model_load_time = 0


def load_model_if_updated():
    global last_model_load_time, model
    try:
        if not os.path.exists(MODEL_PATH): return
        mtime = os.path.getmtime(MODEL_PATH)
        if mtime > last_model_load_time:
            model.load_model(MODEL_PATH)
            last_model_load_time = mtime
            logger.info("🔄 Model reloaded (Hot Swap)")
    except Exception:
        pass


def get_db_conn():
    return psycopg2.connect(host=PG_HOST, user=PG_USER, password=PG_PASS, dbname=PG_DB)


def predict_future(ticker, horizon_hours):
    load_model_if_updated()
    conn = get_db_conn()

    # Берем данные за последние 2-3 дня, чтобы хватило на расчет SMA (24 часа)
    # Используем NOW(), так как у нас Live-система
    query = f"""
        SELECT ticker, close, volume, rsi, macd, day_of_week, time_of_day, ts
        FROM market_candles
        WHERE ticker = '{ticker}' AND ts > NOW() - INTERVAL '3 days'
        ORDER BY ts ASC
    """
    try:
        df = pd.read_sql(query, conn)
    except:
        conn.close();
        return []

    if len(df) < 30:
        logger.warning(f"Not enough data for {ticker}")
        conn.close();
        return []

    predictions = []

    # Подготовка к рекурсии
    # Нам нужно рассчитать SMA и Returns на лету
    # Берем хвост датафрейма как буфер

    current_price = df.iloc[-1]['close']
    current_ts = df.iloc[-1]['ts']

    # Буфер для расчета SMA (нам нужно окно 24)
    close_history = list(df['close'].values)

    # Статичные параметры (упрощение: RSI/Volume берем последние, считаем что не меняются)
    last_vol = df.iloc[-1]['volume']
    last_rsi = df.iloc[-1]['rsi']
    last_macd = df.iloc[-1]['macd']

    for i in range(horizon_hours):
        next_ts = current_ts + timedelta(hours=i + 1)

        # 1. Считаем фичи на лету
        # SMA 24
        sma_24 = np.mean(close_history[-24:])
        dist_to_sma = current_price / sma_24

        # Returns (изменения цены за прошлые шаги)
        return_lag1 = close_history[-1] / close_history[-2]
        return_lag2 = close_history[-2] / close_history[-3]

        features = pd.DataFrame([{
            'ticker': ticker,
            'volume': last_vol,
            'rsi': last_rsi,
            'macd': last_macd,
            'day_of_week': next_ts.weekday(),
            'time_of_day': next_ts.hour,
            'return_lag1': return_lag1,
            'return_lag2': return_lag2,
            'dist_to_sma': dist_to_sma
        }])

        # 2. ПРЕДСКАЗАНИЕ (Модель дает Ratio, например 1.002)
        pred_ratio = model.predict(features)[0]

        # 3. Конвертация в Цену: New_Price = Old_Price * Ratio
        next_price = current_price * pred_ratio

        predictions.append({
            'ticker': ticker, 'ts': next_ts, 'price': float(next_price), 'horizon': f"{i + 1} h"
        })

        # Обновляем состояние для следующего шага
        close_history.append(next_price)  # Добавляем предсказанную цену в историю для SMA
        current_price = next_price

    conn.close()
    return predictions


def start_consuming():
    time.sleep(15)  # Wait for Kafka
    consumer = KafkaConsumer(
        'predict_requests', bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset='latest', enable_auto_commit=True,
        group_id='ml_worker_group', value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    logger.info("✅ Live Worker Started")

    for message in consumer:
        task = message.value
        try:
            start_t = time.time()
            preds = predict_future(task['ticker'], int(task['horizon']))
            dur = int((time.time() - start_t) * 1000)

            if not preds: continue

            conn = get_db_conn()
            cur = conn.cursor()

            cur.execute("DELETE FROM model_predictions WHERE ticker = %s", (task['ticker'],))

            recs = [(p['ticker'], p['ts'], p['horizon'], p['price']) for p in preds]
            execute_values(cur, "INSERT INTO model_predictions (ticker, ts, horizon, score) VALUES %s", recs)

            # Логгирование с ценами (для метрик качества)
            start_p = preds[0]['price']
            end_p = preds[-1]['price']

            cur.execute("""
                INSERT INTO prediction_logs (ticker, requested_at, horizon_hours, processing_time_ms, predicted_start_price, predicted_end_price)
                VALUES (%s, NOW(), %s, %s, %s, %s)
            """, (task['ticker'], int(task['horizon']), dur, start_p, end_p))

            conn.commit()
            conn.close()
            logger.info(f"✅ Predicted {task['ticker']} ({dur}ms)")

        except Exception as e:
            logger.error(f"Error: {e}")


if __name__ == "__main__":
    start_consuming()