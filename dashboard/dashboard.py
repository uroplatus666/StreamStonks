import streamlit as st
import pandas as pd
import psycopg2
import json
import os
import time
from datetime import timedelta
import plotly.graph_objects as go
from kafka import KafkaProducer

# --- КОНФИГУРАЦИЯ ---
st.set_page_config(page_title="AI Market Predictor", layout="wide")

PG_HOST = os.getenv("POSTGRES_HOST")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASS = os.getenv("POSTGRES_PASSWORD")
PG_DB = os.getenv("POSTGRES_DB")

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


# --- ФУНКЦИИ ---

def get_db_connection():
    return psycopg2.connect(host=PG_HOST, user=PG_USER, password=PG_PASS, dbname=PG_DB)


def get_system_time():
    """Машина времени: определяем 'сейчас' как последнюю точку во ВСЕЙ базе"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(ts) FROM market_candles")
            result = cur.fetchone()
            if result and result[0]:
                return result[0]
            return None
    finally:
        conn.close()


def get_ticker_last_date(ticker):
    """Узнаем дату последней свечи для КОНКРЕТНОГО тикера"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(ts) FROM market_candles WHERE ticker = %s", (ticker,))
            last_date = cur.fetchone()[0]
        return last_date
    finally:
        conn.close()


def get_tickers_info():
    """Получаем таблицу: Тикер, Название, Сектор (только для тех, у кого есть свечи)"""
    conn = get_db_connection()
    # JOIN нужен, чтобы взять русское название и сектор
    query = """
        SELECT DISTINCT t.symbol, t.company_name, t.sector
        FROM tickers t
        JOIN market_candles m ON t.symbol = m.ticker
        ORDER BY t.sector, t.symbol
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def get_history(ticker, system_time):
    """Берем историю ТОЛЬКО до системного времени"""
    conn = get_db_connection()
    query = f"""
        SELECT ts, close 
        FROM market_candles 
        WHERE ticker = '{ticker}' AND ts <= '{system_time}'
        ORDER BY ts ASC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def get_predictions(ticker, system_time):
    """Берем прогнозы, которые идут ПОСЛЕ системного времени"""
    conn = get_db_connection()
    query = f"""
        SELECT ts, score as close, horizon
        FROM model_predictions
        WHERE ticker = '{ticker}' AND ts > '{system_time}'
        ORDER BY ts ASC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def send_task_to_kafka(ticker, horizon_hours, system_time):
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

        message = {
            'ticker': ticker,
            'date': str(system_time),
            'horizon': horizon_hours
        }

        producer.send('predict_requests', value=message)
        producer.flush()
        producer.close()
        return True
    except Exception as e:
        st.error(f"Ошибка Kafka: {e}")
        return False


# --- ИНТЕРФЕЙС STREAMLIT ---

st.title("📈 AI Market Predictor (Enterprise Demo)")

# 1. Определяем глобальное время
try:
    SYSTEM_TIME = get_system_time()
    if SYSTEM_TIME is None:
        st.error("База данных пуста! Запустите ingest.py")
        st.stop()

    st.markdown(f"### 🕒 System Date: **{SYSTEM_TIME.strftime('%Y-%m-%d %H:%M')}**")
    st.info("ℹ️ Система работает в режиме симуляции на исторических данных.")
except Exception as e:
    st.error(f"Ошибка подключения к БД: {e}")
    st.stop()

# 2. Боковая панель
with st.sidebar:
    st.header("Настройки")

    # --- НОВАЯ ЛОГИКА ВЫБОРА АКТИВА ---
    df_tickers = get_tickers_info()

    if df_tickers.empty:
        st.warning("Нет данных о тикерах.")
        st.stop()

    # 1. Выбор сектора
    sectors = df_tickers['sector'].unique()
    selected_sector = st.selectbox("📂 Выберите сектор", sectors)

    # 2. Фильтрация акций по сектору
    sector_stocks = df_tickers[df_tickers['sector'] == selected_sector]

    # Создаем словарь для отображения: "LKOH (Лукойл)" -> "LKOH"
    stock_options = {
        f"{row.symbol} ({row.company_name})": row.symbol
        for row in sector_stocks.itertuples()
    }

    selected_display_name = st.selectbox("📊 Выберите актив", list(stock_options.keys()))

    # Получаем чистый тикер для запросов (например, "LKOH")
    selected_ticker = stock_options[selected_display_name]

    # --- ПРОВЕРКА АКТУАЛЬНОСТИ ДАННЫХ ---
    last_ticker_date = get_ticker_last_date(selected_ticker)

    is_active = True
    status_message = ""

    if last_ticker_date:
        diff = SYSTEM_TIME - last_ticker_date
        # Если данные старее 7 дней относительно "сегодня"
        if diff.days > 7:
            is_active = False
            status_message = f"Торги остановлены {last_ticker_date.strftime('%Y-%m-%d')}"
    else:
        is_active = False  # Данных вообще нет

    # --- UI ЭЛЕМЕНТЫ ---
    horizon_days = st.slider("Горизонт прогноза (дней)", 1, 14, 7)

    if is_active:
        if st.button("🚀 Сделать прогноз"):
            with st.spinner('Отправка задачи в ML-кластер...'):
                if send_task_to_kafka(selected_ticker, horizon_days * 24, SYSTEM_TIME):
                    st.success(f"Задача для {selected_ticker} принята!")

                    # Polling
                    progress_bar = st.progress(0)
                    for i in range(15):
                        time.sleep(1)
                        progress_bar.progress((i + 1) * 6)
                        preds_check = get_predictions(selected_ticker, SYSTEM_TIME)
                        if not preds_check.empty:
                            st.success("Готово!")
                            time.sleep(0.5)
                            st.rerun()
                            break
                    else:
                        st.warning("Воркер долго думает. Обновите страницу.")
    else:
        st.error("⛔ ПРОГНОЗ НЕДОСТУПЕН")
        st.warning(f"Причина: {status_message}")
        st.caption("Данные по этому активу устарели. Возможно, компания прошла реструктуризацию или делистинг.")

# 3. Основная область (Графики)
col1, col2 = st.columns([3, 1])

with col1:
    history_df = get_history(selected_ticker, SYSTEM_TIME)
    pred_df = get_predictions(selected_ticker, SYSTEM_TIME)

    fig = go.Figure()

    # Историческая линия
    fig.add_trace(go.Scatter(
        x=history_df['ts'],
        y=history_df['close'],
        mode='lines',
        name='История',
        line=dict(color='#1f77b4', width=2)
    ))

    # Прогноз (если есть и активен)
    if not pred_df.empty:
        last_hist = history_df.iloc[-1]

        # Мостик
        fig.add_trace(go.Scatter(
            x=[last_hist['ts'], pred_df.iloc[0]['ts']],
            y=[last_hist['close'], pred_df.iloc[0]['close']],
            mode='lines',
            line=dict(color='#ff7f0e', dash='dot', width=2),
            showlegend=False
        ))

        # Линия прогноза
        fig.add_trace(go.Scatter(
            x=pred_df['ts'],
            y=pred_df['close'],
            mode='lines',
            name='AI Прогноз',
            line=dict(color='#ff7f0e', dash='dot', width=2)
        ))

        # Точка
        final_pt = pred_df.iloc[-1]
        fig.add_trace(go.Scatter(
            x=[final_pt['ts']], y=[final_pt['close']],
            mode='markers+text',
            marker=dict(size=10, color='red'),
            text=[f"{final_pt['close']:.1f}"],
            textposition="top center",
            name='Цель'
        ))

    # Если торги остановлены, добавим вертикальную линию окончания торгов
    if not is_active and last_ticker_date:
        fig.add_vline(x=last_ticker_date.timestamp() * 1000, line_width=2, line_dash="dash", line_color="red")
        fig.add_annotation(x=last_ticker_date, y=history_df['close'].iloc[-1],
                           text="Остановка торгов", showarrow=True, arrowhead=1)

    fig.update_layout(
        title=f"Динамика цены {selected_display_name}",  # Используем красивое имя в заголовке
        xaxis_title="Дата",
        yaxis_title="Цена (RUB)",
        height=600,
        template="plotly_white"
    )
    st.plotly_chart(fig, use_container_width=True)

# 4. Метрики
with col2:
    st.subheader("Показатели")

    if not history_df.empty:
        curr_price = history_df.iloc[-1]['close']
        st.metric("Цена (Latest)", f"{curr_price:.2f} ₽")

    if not is_active:
        st.error("⚠️ ТОРГИ ПРИОСТАНОВЛЕНЫ")

    if not pred_df.empty and is_active:
        fut_price = pred_df.iloc[-1]['close']
        delta = fut_price - curr_price
        delta_pct = (delta / curr_price) * 100

        st.metric(
            f"Прогноз (+{horizon_days} дн.)",
            f"{fut_price:.2f} ₽",
            delta=f"{delta:.2f} ₽ ({delta_pct:.2f}%)"
        )

        if delta > 0:
            st.success("Рекомендация: **ПОКУПАТЬ**")
        else:
            st.error("Рекомендация: **ПРОДАВАТЬ**")