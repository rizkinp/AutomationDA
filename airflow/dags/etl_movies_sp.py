from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import mysql.connector
import pandas as pd
import logging
import gspread as gs

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

BATCH_SIZE = 500

# Koneksi Database
def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)

# Fungsi untuk mendapatkan offset terakhir dari process_log
def get_last_offset():
    with get_db_connection() as conn, conn.cursor() as cursor:
        cursor.execute("SELECT last_offset FROM process_log ORDER BY id DESC LIMIT 1")
        result = cursor.fetchone()
        return result[0] if result else 0  # Jika tidak ada record, mulai dari 0

# Fungsi untuk memperbarui offset terakhir setelah proses selesai
def update_last_offset(offset):
    with get_db_connection() as conn, conn.cursor() as cursor:
        cursor.execute("INSERT INTO process_log (last_offset) VALUES (%s)", (offset,))
        conn.commit()

# Load data dari Google Sheets ke staging
def load_to_staging():
    logging.info("Loading data ke staging...")
    
    df = pd.DataFrame(gs.service_account('/opt/airflow/dags/data-analyst-447306-7753769f723a.json')
                      .open_by_key('1ZNVJziTv1Krl3DUEmpcIsVTaabeiFFSRk02q400Guk4')
                      .worksheet('Online Retail Data')
                      .get_all_records())

    df.fillna('', inplace=True)
    # Tentukan kolom numerik
    numeric_cols = ['order_id', 'quantity', 'price', 'customer_id']

    # Konversi kolom numerik ke tipe numerik dan ganti NaN dengan 0
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    df[numeric_cols] = df[numeric_cols].fillna(0)
    df[numeric_cols] = df[numeric_cols].astype(float)

    # Debug: Cek apakah masih ada NaN
    logging.info("Jumlah NaN per kolom sebelum insert:\n%s", df.isna().sum())

    with get_db_connection() as conn, conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders_staging (
                order_id INT PRIMARY KEY, product_code VARCHAR(50),
                product_name VARCHAR(255), quantity INT, order_date DATE,
                price FLOAT, customer_id INT
            );
        """)

        batch_size = 50  # Batasi hanya 10 baris per batch saat `executemany()`
        data_list = df.values.tolist()

        for i in range(0, len(data_list), batch_size):
            cursor.executemany("""
                INSERT INTO orders_staging VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE product_code=VALUES(product_code),
                product_name=VALUES(product_name), quantity=VALUES(quantity),
                order_date=VALUES(order_date), price=VALUES(price), customer_id=VALUES(customer_id);
            """, data_list[i:i + batch_size])
            conn.commit()
    
    logging.info("Data berhasil dimuat ke staging.")

# Transformasi dan Load ke tabel bersih dalam batch berdasarkan offset
def transform_and_load():
    logging.info("Transformasi data...")

    offset = get_last_offset()  # Ambil offset terakhir yang telah diproses

    with get_db_connection() as conn:
        df = pd.read_sql(f"SELECT * FROM orders_staging LIMIT {BATCH_SIZE} OFFSET {offset}", conn)
    
    if df.empty:
        logging.info("Tidak ada data baru untuk diproses.")
        return

    # Bersihkan data
    df_cleaned = df.dropna().drop_duplicates()
    df_cleaned = df_cleaned[df_cleaned['quantity'] > 0]
    df_cleaned['product_name'] = df_cleaned['product_name'].str.strip().str.title()

    with get_db_connection() as conn, conn.cursor() as cursor:
        cursor.execute("CREATE TABLE IF NOT EXISTS orders_cleaned LIKE orders_staging;")
        cursor.executemany("""
            INSERT INTO orders_cleaned VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE product_code=VALUES(product_code),
            product_name=VALUES(product_name), quantity=VALUES(quantity),
            order_date=VALUES(order_date), price=VALUES(price), customer_id=VALUES(customer_id);
        """, df_cleaned.values.tolist())
        conn.commit()

    new_offset = offset + BATCH_SIZE
    update_last_offset(new_offset)  # Simpan offset terbaru setelah proses selesai

    logging.info(f"Processed records {offset} - {new_offset}")

# Konfigurasi DAG
default_args = {'owner': 'airflow', 'start_date': datetime(2025, 1, 29), 'retries': 1}
dag = DAG('etl_movies_sp', default_args=default_args, schedule_interval='@daily', catchup=False)

# Tugas dalam DAG
task_load_staging = PythonOperator(task_id='load_to_staging', python_callable=load_to_staging, dag=dag)
task_transform_and_load = PythonOperator(task_id='transform_and_load', python_callable=transform_and_load, dag=dag)

task_load_staging >> task_transform_and_load
