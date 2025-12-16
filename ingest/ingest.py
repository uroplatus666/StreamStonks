import os
import logging
import time
import schedule
from datetime import datetime, timedelta, timezone
import pandas as pd
import ta
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import keyring
from keyrings.alt import file
from TinvestPy import TinvestPy
from TinvestPy.grpc.marketdata_pb2 import GetCandlesRequest, CandleInterval

# Настройка
keyring.set_keyring(file.PlaintextKeyring())
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("IngestWorker")

TOKEN = os.environ.get("TINKOFF_TOKEN")
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_USER = os.getenv("POSTGRES_USER", "mluser")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "mlpass")
PG_DB = os.getenv("POSTGRES_DB", "marketdb")

# --- СПИСОК ТИКЕРОВ ---
TICKERS_META = {
    # Нефтегаз
    "GAZP": ("Газпром", "Energy"),
    "LKOH": ("Лукойл", "Energy"),
    "ROSN": ("Роснефть", "Energy"),
    "NVTK": ("Новатэк", "Energy"),
    "TATN": ("Татнефть", "Energy"),
    "SNGS": ("Сургутнефтегаз", "Energy"),
    "SNGSP": ("Сургутнефтегаз (преф)", "Energy"),
    "SIBN": ("Газпром нефть", "Energy"),
    "TRNFP": ("Транснефть", "Energy"),
    "BANE": ("Башнефть", "Energy"),

    # Финансы
    "SBER": ("Сбербанк", "Financial"),
    "SBERP": ("Сбербанк (преф)", "Financial"),
    "VTBR": ("ВТБ", "Financial"),
    # "TCSG": ("ТКС Холдинг", "Financial"),
    "MOEX": ("Московская Биржа", "Financial"),
    "CBOM": ("МКБ", "Financial"),
    "BSPB": ("Банк Санкт-Петербург", "Financial"),

    # Металлургия и добыча
    "GMKN": ("Норникель", "Materials"),
    "PLZL": ("Полюс", "Materials"),
    "ALRS": ("Алроса", "Materials"),
    "RUAL": ("Русал", "Materials"),
    "CHMF": ("Северсталь", "Materials"),
    "NLMK": ("НЛМК", "Materials"),
    "MAGN": ("ММК", "Materials"),
    "PHOR": ("ФосАгро", "Materials"),
    "SELG": ("Селигдар", "Materials"),

    # IT и Телеком
    "YDEX": ("Яндекс МКПАО", "Technology"),
    "MTSS": ("МТС", "Telecom"),
    "RTKM": ("Ростелеком", "Telecom"),
    "VKCO": ("VK", "Technology"),
    "POSI": ("Positive Technologies", "Technology"),
    "OZON": ("Ozon", "Consumer Cyclical"),
    "ASTR": ("Астра", "Technology"),

    # Потребительский сектор
    "MGNT": ("Магнит", "Consumer Defensive"),
    "FIVE": ("X5 Group", "Consumer Defensive"),
    "BELU": ("Novabev (Белуга)", "Consumer Defensive"),
    "AQUA": ("Инарктика", "Consumer Defensive"),

    # Медицина (Healthcare)
    "MDMG": ("Мать и дитя", "Healthcare"),
    "ABIO": ("Артген биотех", "Healthcare"),
    "GEMC": ("ЕвроМедЦентр", "Healthcare"),

    # Недвижимость
    "PIKK": ("ПИК", "Real Estate"),
    "SMLT": ("Самолет", "Real Estate"),
    "LSRG": ("ЛСР", "Real Estate"),

    # Электроэнергетика
    "IRAO": ("Интер РАО", "Utilities"),
    "HYDR": ("РусГидро", "Utilities"),
    "FEES": ("ФСК - Россети", "Utilities"),
    "MSNG": ("Мосэнерго", "Utilities"),

    # Транспорт
    "AFLT": ("Аэрофлот", "Industrials"),
    "FLOT": ("Совкомфлот", "Industrials")
}



def get_db_connection():
    return psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASS)


def init_tickers_metadata():
    """Заполняет справочник тикеров"""
    conn = get_db_connection()
    cur = conn.cursor()
    query = """
        INSERT INTO tickers (symbol, company_name, sector) VALUES %s
        ON CONFLICT (symbol) DO UPDATE SET company_name = EXCLUDED.company_name;
    """
    data = [(t, m[0], m[1]) for t, m in TICKERS_META.items()]
    execute_values(cur, query, data)
    conn.commit()
    conn.close()


def get_latest_timestamp(ticker):
    """Узнаем, когда была последняя запись в БД для этого тикера"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT MAX(ts) FROM market_candles WHERE ticker = %s", (ticker,))
    res = cur.fetchone()
    conn.close()
    if res and res[0]:
        return res[0]
    return None


def get_figi_by_ticker(provider, ticker):
    try:
        return provider.get_symbol_info('TQBR', ticker).figi
    except:
        return None


def download_candles_chunked(provider, figi, start, end):
    """Качает свечи чанками по 7 дней, чтобы API не ругался"""
    interval = CandleInterval.CANDLE_INTERVAL_HOUR
    all_candles = []

    current_start = start
    # API Тинькофф лучше переваривает запросы по неделям
    chunk_size = timedelta(days=7)

    while current_start < end:
        current_end = min(current_start + chunk_size, end)

        req = GetCandlesRequest(instrument_id=figi, interval=interval)
        getattr(req, 'from').seconds = int(current_start.timestamp())
        getattr(req, 'to').seconds = int(current_end.timestamp())

        try:
            resp = provider.call_function(provider.stub_marketdata.GetCandles, req)
            if resp.candles:
                for c in resp.candles:
                    ts = datetime.fromtimestamp(c.time.seconds, timezone.utc)
                    all_candles.append({
                        "ts": ts, "open": provider.quotation_to_float(c.open),
                        "high": provider.quotation_to_float(c.high), "low": provider.quotation_to_float(c.low),
                        "close": provider.quotation_to_float(c.close), "volume": c.volume
                    })
        except Exception as e:
            logger.warning(f"Chunk failed {current_start}: {e}")
            time.sleep(1)  # Пауза при ошибке
            
        time.sleep(0.2)
        current_start = current_end

    return pd.DataFrame(all_candles)


def process_data(df):
    if df.empty: return df
    df = df.sort_values('ts').drop_duplicates(subset=['ts']).reset_index(drop=True)

    # Technical Analysis
    df['rsi'] = ta.momentum.rsi(df['close'], window=14)
    df['macd'] = ta.trend.macd_diff(df['close'])

    df = df.dropna()
    return df


def save_to_db(ticker, df):
    if df.empty: return
    conn = get_db_connection()
    cur = conn.cursor()
    records = []
    for row in df.itertuples():
        records.append(
            (ticker, row.ts, row.open, row.high, row.low, row.close, row.volume, row.macd, row.rsi, row.ts.weekday(),
             row.ts.hour))

    query = """
        INSERT INTO market_candles (ticker, ts, open, high, low, close, volume, macd, rsi, day_of_week, time_of_day)
        VALUES %s ON CONFLICT (ticker, ts) DO NOTHING
    """
    execute_values(cur, query, records)
    conn.commit()
    conn.close()


def sync_tickers(backfill_years=5):
    """Умная синхронизация: качает историю ИЛИ докачивает свежее"""
    logger.info("🔄 STARTING SYNC PROCESS...")

    if not TOKEN:
        logger.error("No Token!")
        return

    try:
        provider = TinvestPy(token=TOKEN)
        now_time = datetime.now(timezone.utc)

        for ticker in TICKERS_META.keys():
            figi = get_figi_by_ticker(provider, ticker)
            if not figi:
                logger.warning(f"Skipping {ticker}: FIGI not found")
                continue

            # 1. Проверяем, что есть в БД
            last_ts = get_latest_timestamp(ticker)

            if last_ts:
                # Если данные есть -> качаем с последнего момента до сейчас
                start_time = last_ts
                mode = "LIVE UPDATE"
            else:
                # Если данных нет -> качаем 5 лет истории (Backfill)
                start_time = now_time - timedelta(days=365 * backfill_years)
                mode = "FULL BACKFILL (5 Years)"

            # Если данные слишком свежие, не мучаем API
            if (now_time - start_time).total_seconds() < 600:
                logger.info(f"⏭️ {ticker} is up to date.")
                continue

            logger.info(f"📥 {mode} for {ticker}: {start_time} -> {now_time}")

            df = download_candles_chunked(provider, figi, start_time, now_time)

            if not df.empty:
                df = process_data(df)
                save_to_db(ticker, df)
                logger.info(f"✅ Saved {len(df)} rows for {ticker}")
            else:
                logger.info(f"⚠️ No new data for {ticker}")

            # Небольшая пауза, чтобы не убить API лимитами
            time.sleep(0.5)

        provider.close_channel()
        logger.info("🏁 SYNC COMPLETE")

    except Exception as e:
        logger.error(f"Sync failed: {e}")


# Обертка для планировщика (всегда смотрит назад на 3 дня для надежности)
def scheduled_job():
    # В режиме Live нам не нужно смотреть на 5 лет назад каждый раз.
    # Достаточно проверять последние 3-7 дней, чтобы перекрыть выходные/праздники
    # Логика sync_tickers сама разберется через get_latest_timestamp,
    # но чтобы не делать лишних запросов count(), можно просто вызывать sync_tickers
    sync_tickers(backfill_years=5)


if __name__ == "__main__":
    # 1. ЖДЕМ ПОДКЛЮЧЕНИЯ К БД
    logger.info("⏳ Waiting for Database connection...")
    while True:
        try:
            init_tickers_metadata()
            logger.info("✅ Database connection established.")
            break
        except Exception as e:
            logger.warning(f"Database not ready yet ({e}). Retrying in 5s...")
            time.sleep(5)

    # 2. ПЕРВЫЙ ЗАПУСК
    logger.info("🚀 Initializing Data Load...")
    sync_tickers(backfill_years=5)

    # 3. Запуск расписания
    schedule.every(10).minutes.do(scheduled_job)

    logger.info("⏰ Scheduler started. Waiting for next window...")
    while True:
        schedule.run_pending()
        time.sleep(1)