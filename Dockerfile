# ВАЖНО: TinvestPy требует Python >= 3.12
FROM python:3.12-slim

WORKDIR /app

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    python3-dev \
    build-essential \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 1. Устанавливаем библиотеки
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements.txt

# 2. Копируем ВСЕ скрипты проекта 

# Ingest
COPY ingest/ingest.py .

# ML Service
COPY ml/ml_worker.py .

# Dashboard
COPY dashboard/dashboard.py .

# ML Service
COPY ml/train_model.py .
COPY ml/scheduler.py .

# Копируем схему БД (полезно для отладки)
COPY db_init/schema.sql .

# 3. ВАЖНО: Копируем файл обученной модели (начальная версия)
COPY ml/catboost_model.cbm .

# Создаем папку для обмена моделью между сервисами
RUN mkdir -p /app/shared_models && cp catboost_model.cbm /app/shared_models/catboost_model.cbm

# По умолчанию запускаем bash
CMD ["bash"]