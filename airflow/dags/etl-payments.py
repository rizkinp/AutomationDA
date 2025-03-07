from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pymysql
import pandas as pd
import logging

# Database konfigurasi
DB_SOURCE = {
    "host": "23.92.208.186",
    "port": 3306,
    "user": "dashboard",
    "password": "Q+TYxA*T+t6A58OD4F-2xbh1h:Orq}1A",
    "database": "dashboard",
}

DB_TARGET = {
    "host": "109.123.234.160",
    "port": 3306,
    "user": "mysql",
    "password": "hzIDKT6NJNaISCJmVLKS9yHXPxBODxXdtchOf6IJYXXLPKnUvIp9VciW2RzRtor2",
    "database": "default",
}

# Koneksi database
def get_db_connection(db_config):
    return pymysql.connect(**db_config)

# Proses ETL
def etl_process():
    logging.info("🔹 Mengambil data dari database sumber...")
    conn_source = get_db_connection(DB_SOURCE)
    df = pd.read_sql("SELECT * FROM payment", con=conn_source)
    conn_source.close()

    logging.info("✅ Data berhasil diambil!")

    logging.info("🔹 Memproses data...")

    # Membersihkan data
    columns_to_remove = ["updated_by", "created_at", "updated_at"]
    df_cleaned = df.drop(columns=columns_to_remove, errors="ignore")

    df_cleaned.fillna({
        "id_auto_payment": 0, "id_manual_payment": 0, "id_idn_payment": 0, 
        "id_briva_payment": 0, "nominal": 0, "edcCost": 0, "description": "UNKNOWN",
        "bank": "UNKNOWN", "method": "UNKNOWN", "channel": "UNKNOWN"
    }, inplace=True)

    num_columns = ["id", "id_auto_payment", "id_manual_payment", "id_idn_payment", "id_briva_payment", "nominal", "edcCost"]
    for col in num_columns:
        df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors="coerce")

    if "date" in df_cleaned.columns:
        df_cleaned["date"] = pd.to_datetime(df_cleaned["date"], errors="coerce")

    df_cleaned = df_cleaned.drop_duplicates(subset=["id"])

    logging.info("✅ Data berhasil dibersihkan!")

    # Koneksi ke database tujuan
    conn_target = get_db_connection(DB_TARGET)
    cursor_target = conn_target.cursor()

    cursor_target.execute("SELECT id FROM payments_cleaned")
    existing_ids = set(row[0] for row in cursor_target.fetchall())

    # 🔹 Filter hanya data baru
    df_new = df_cleaned[~df_cleaned["id"].isin(existing_ids)]
    
    logging.info(f"🔍 Data baru yang ditemukan: {len(df_new)} record(s)")

    if not df_new.empty:
        cleaned_tuples = [tuple(x) for x in df_new.itertuples(index=False, name=None)]
        sql_insert = """
        INSERT IGNORE INTO payments_cleaned 
        (id, id_auto_payment, id_manual_payment, id_idn_payment, id_briva_payment, date, regcode, type, channel, method, bank, nominal, edcCost, description) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor_target.executemany(sql_insert, cleaned_tuples)
        conn_target.commit()
        logging.info(f"✅ {len(df_new)} data baru berhasil dimasukkan ke database!")
    else:
        logging.info("✅ Tidak ada data baru untuk dimasukkan hari ini.")

    conn_target.close()

    logging.info("✅ Proses ETL selesai!")

# Fungsi untuk menghitung jumlah data di tabel target
def count_records():
    conn_target = get_db_connection(DB_TARGET)
    cursor_target = conn_target.cursor()

    cursor_target.execute("SELECT COUNT(*) FROM payments_cleaned")
    total_records = cursor_target.fetchone()[0]


    print(f"✅ Total records in payments_cleaned: {total_records}")
    logging.info(f"✅ Total records in payments_cleaned: {total_records}")

    conn_target.close()
    return total_records

# Setup DAG
dag = DAG(
    'etl_payments',
    default_args={'owner': 'airflow', 'start_date': datetime(2025, 1, 29)},
    schedule_interval='@daily',
    catchup=False
)

# Task ETL
task_etl = PythonOperator(
    task_id='etl_process',
    python_callable=etl_process,
    dag=dag
)

# Task Hitung Data
task_count = PythonOperator(
    task_id='count_records',
    python_callable=count_records,
    dag=dag
)

# Menjalankan task_count setelah ETL selesai
task_etl >> task_count
