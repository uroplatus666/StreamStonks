import pandas as pd
import psycopg2
import os
import numpy as np
from catboost import CatBoostRegressor
from sklearn.model_selection import train_test_split
from dotenv import load_dotenv

load_dotenv()
PG_HOST = os.getenv("POSTGRES_HOST")  # localhost для ручного запуска
PG_USER = os.getenv("POSTGRES_USER")
PG_PASS = os.getenv("POSTGRES_PASSWORD")
PG_DB = os.getenv("POSTGRES_DB")


def get_data():
    print("⏳ Loading data...")
    conn = psycopg2.connect(host=PG_HOST, user=PG_USER, password=PG_PASS, dbname=PG_DB)
    query = "SELECT ticker, ts, close, volume, rsi, macd, day_of_week, time_of_day FROM market_candles ORDER BY ticker, ts ASC"
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def prepare_features(df):
    print("🛠 Feature Engineering...")

    # 1. Target: Предсказываем ОТНОШЕНИЕ цены следующего часа к текущему (Ratio)
    # Пример: было 100, стало 101 -> target = 1.01
    df['target_ratio'] = df.groupby('ticker')['close'].shift(-1) / df['close']

    # 2. Фичи: Лаги изменений (Returns Lags)
    # Насколько цена изменилась за прошлый час?
    df['return_lag1'] = df['close'] / df.groupby('ticker')['close'].shift(1)
    df['return_lag2'] = df['close'] / df.groupby('ticker')['close'].shift(2)

    # 3. Скользящие средние (Trend)
    # Отношение текущей цены к средней за 24 часа (где мы относительно тренда?)
    df['sma_24'] = df.groupby('ticker')['close'].transform(lambda x: x.rolling(24).mean())
    df['dist_to_sma'] = df['close'] / df['sma_24']

    df = df.dropna()
    return df


def train_model():
    df = get_data()
    if df.empty: return

    df = prepare_features(df)

    # Убираем лишнее. Target теперь 'target_ratio', а не цена!
    X = df.drop(columns=['ts', 'target_ratio', 'close', 'sma_24'])
    y = df['target_ratio']

    print(f"Features: {X.columns.tolist()}")

    # Сплит (без перемешивания во времени!)
    split_idx = int(len(X) * 0.9)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    print("🧠 Training CatBoost on PRICE RATIOS...")
    model = CatBoostRegressor(iterations=1000, depth=6, learning_rate=0.03, loss_function='RMSE', verbose=100)

    model.fit(X_train, y_train, cat_features=['ticker'], eval_set=(X_test, y_test))

    model_path = "shared_models/catboost_model.cbm"
    # Если запускаем локально без докера, сохраняем рядом
    if not os.path.exists("shared_models"):
        os.makedirs("shared_models", exist_ok=True)

    model.save_model(model_path)
    print(f"🎉 Model updated and saved to {model_path}!")


if __name__ == "__main__":
    train_model()