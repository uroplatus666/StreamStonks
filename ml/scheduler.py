import time
import schedule
import logging
from train_model import train_model

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Scheduler")


def job():
    logger.info("⏰ Starting scheduled retraining...")
    try:
        train_model()
        logger.info("✅ Retraining complete.")
    except Exception as e:
        logger.error(f"❌ Retraining failed: {e}")


if __name__ == "__main__":
    logger.info("Scheduler started. Plan: Every 12 hours.")

    # Каждые 12 часов
    schedule.every(12).hours.do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)