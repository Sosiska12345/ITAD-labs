"""DAG ЛР № 2: Имитация скачивания HTTP-источника (Fallback) и загрузка в SeaweedFS."""

import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Считываем параметры датасета
DATASET_SLUG = os.getenv("DATASET_SLUG", "hour_data")
SOURCE_FILENAME = os.getenv("SOURCE_FILENAME", "hour.csv")
S3_CONN_ID = os.getenv("S3_CONN_ID", "s3_conn")
RAW_BUCKET = os.getenv("RAW_BUCKET", "raw")

def load_to_raw_fallback(**kwargs):
    """
    Fallback-функция: читает зафиксированный локальный снимок и загружает в SeaweedFS.
    """
    ds = "2026-09-16"
    s3_key = f"{DATASET_SLUG}/ingested_on={ds}/{SOURCE_FILENAME}"
    
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    
    # 1. Проверка существования ключа для предотвращения дубликатов (Идемпотентность)
    if s3_hook.check_for_key(key=s3_key, bucket_name=RAW_BUCKET):
        print(f" Найдено: Объект s3://{RAW_BUCKET}/{s3_key} уже существует. Пропускаем.")
        return

    # 2. Чтение локального снимка (Имитация Pull без интернета)
    local_path = f"/opt/airflow/dags/{SOURCE_FILENAME}"

    print(f" Чтение локального fallback-файла: {local_path}")
    
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Критическая ошибка: Файл не найден по пути {local_path}. Проверьте Шаг 1.")
        
    with open(local_path, "rb") as f:
        file_bytes = f.read()
    
    # Проверяем наличие бакета
    if not s3_hook.check_for_bucket(RAW_BUCKET):
        s3_hook.create_bucket(RAW_BUCKET)
    
    # 3. Загрузка байтов в бакет через S3 API
    s3_hook.load_bytes(
        bytes_data=file_bytes,
        key=s3_key,
        bucket_name=RAW_BUCKET,
        replace=False
    )
    print(f" Успешно сохранено в SeaweedFS: s3://{RAW_BUCKET}/{s3_key}")

with DAG(
        dag_id="ingest_raw",
        description="ЛР № 2: Воспроизведение загрузки из зафиксированного снимка при недоступности первоисточника.",
        start_date=datetime(2026, 9, 16),
        schedule="@daily",
        catchup=False,
        tags=["raw"],
) as dag:


    load_to_raw_task = PythonOperator(
        task_id="load_to_raw",
        python_callable=load_to_raw_fallback,
        provide_context=True,
    )

    load_to_raw_task
