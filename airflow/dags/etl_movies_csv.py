from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import mysql.connector
import pandas as pd
import logging

# Konfigurasi Database
DB_CONFIG = {
    "host": "109.123.234.160",
    "port": 2080,
    "database": "DataAnalyst",
    "user": "root",
    "password": "lcbisa88",
    "charset": "utf8mb4",
    "collation": "utf8mb4_general_ci",
}

# Fungsi untuk membuat koneksi database
def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)

# Fungsi untuk memuat data ke staging
def load_to_staging():
    logging.info("Memulai proses loading data ke staging...")

    # Baca CSV
    df = pd.read_csv("/opt/airflow/dags/movies-final.csv")

    # Pastikan semua NaN diganti dengan nilai yang sesuai
    df.fillna('', inplace=True)

    # Tentukan kolom numerik
    numeric_cols = ['score', 'votes', 'budget', 'gross', 'runtime']

    # Konversi kolom numerik ke tipe numerik dan ganti NaN dengan 0
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    df[numeric_cols] = df[numeric_cols].fillna(0)
    df[numeric_cols] = df[numeric_cols].astype(float)

    # Debug: Cek apakah masih ada NaN
    logging.info("Jumlah NaN per kolom sebelum insert:\n%s", df.isna().sum())

    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            # Buat tabel jika belum ada
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS movies_staging (
                    name VARCHAR(255), rating VARCHAR(10), genre VARCHAR(50), year INT,
                    released VARCHAR(50), score FLOAT, votes INT, director VARCHAR(255),
                    writer VARCHAR(255), star VARCHAR(255), country VARCHAR(50),
                    budget FLOAT, gross FLOAT, company VARCHAR(255), runtime FLOAT
                );
            """)

            sql_insert = """
                INSERT INTO movies_staging VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            cursor.executemany(sql_insert, df.values.tolist())
            connection.commit()

    logging.info("Data berhasil dimuat ke staging.")

# Fungsi untuk transformasi dan load data ke tabel bersih
def transform_and_load():
    logging.info("Memulai proses transformasi data...")

    with get_db_connection() as connection:
        df = pd.read_sql("SELECT * FROM movies_staging", connection)

    # Bersihkan data: hapus duplikat dan data yang tidak valid
    df_cleaned = df.dropna().drop_duplicates()
    df_cleaned = df_cleaned[(df_cleaned['budget'] > 0) & (df_cleaned['gross'] > 0)]
    df_cleaned['genre'] = df_cleaned['genre'].str.strip().str.title()

    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS movies_cleaned;")
            cursor.execute("""
                CREATE TABLE movies_cleaned AS SELECT * FROM movies_staging WHERE 1=0;
            """)
            sql_insert = """
                INSERT INTO movies_cleaned VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            cursor.executemany(sql_insert, df_cleaned.values.tolist())
            connection.commit()

    logging.info("Transformasi dan load data selesai.")

# Konfigurasi DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 29),
    'retries': 1,
}

dag = DAG('etl_movies_csv', default_args=default_args, schedule_interval='@daily', catchup=False)

task_load_staging = PythonOperator(
    task_id='load_to_staging', 
    python_callable=load_to_staging, 
    dag=dag
)

task_transform_and_load = PythonOperator(
    task_id='transform_and_load', 
    python_callable=transform_and_load, 
    dag=dag
)

task_load_staging >> task_transform_and_load