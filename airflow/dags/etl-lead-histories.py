from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pymysql
import pandas as pd
import logging

# **Konfigurasi Database**
DB_SOURCE = {
    "host": "103.74.5.92",
    "port": 5001,
    "user": "root",
    "password": "cd703b0008b7ba61",
    "database": "lead_service",
}

DB_TARGET = {
    "host": "109.123.234.160",
    "port": 3306,
    "user": "mysql",
    "password": "hzIDKT6NJNaISCJmVLKS9yHXPxBODxXdtchOf6IJYXXLPKnUvIp9VciW2RzRtor2",
    "database": "default",
}

# **Fungsi untuk koneksi database**
def get_db_connection(db_config):
    return pymysql.connect(**db_config)

# **Fungsi untuk Membuat Tabel Jika Belum Ada**
def create_table_if_not_exists():
    logging.info("🔹 Mengecek apakah tabel lead_histories_cleaned sudah ada di MySQL...")

    conn_target = get_db_connection(DB_TARGET)
    cursor_target = conn_target.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS lead_histories_cleaned (
        id INT AUTO_INCREMENT PRIMARY KEY,
        lead_id INT NOT NULL,
        course_package_name TEXT,
        branch_id INT,
        utm_campaign TEXT,
        utm_source TEXT,
        utm_medium TEXT,
        utm_content TEXT,
        response_info TEXT,
        lead_status_id INT,
        lead_category_id INT,
        affiliate TEXT,
        description TEXT,
        source TEXT,
        created_at TIMESTAMP NULL DEFAULT NULL,
        updated_at TIMESTAMP NULL DEFAULT NULL,
        deleted_at TIMESTAMP NULL DEFAULT NULL,
        created_by TEXT,
        updated_by TEXT,
        deleted_by TEXT,
        registration_id VARCHAR(21),
        registration_date TIMESTAMP NULL DEFAULT NULL,
        payment_date TIMESTAMP NULL DEFAULT NULL,
        is_bio_filled BOOLEAN DEFAULT FALSE,
        bank TEXT,
        period_date TIMESTAMP NULL DEFAULT NULL,
        cs_group_id TEXT,
        cs_team_id TEXT,
        customer_service_id TEXT,
        is_replied BOOLEAN DEFAULT FALSE,
        name TEXT
    );
    """

    cursor_target.execute(create_table_query)
    conn_target.commit()
    conn_target.close()

    logging.info("✅ Tabel lead_histories_cleaned sudah tersedia atau berhasil dibuat!")

# **Proses ETL**
def etl_process():
    logging.info("🔹 Memastikan tabel tujuan tersedia...")
    create_table_if_not_exists()  # 🔹 Pastikan tabel ada sebelum memasukkan data

    logging.info("🔹 Mengambil data dari PostgreSQL...")

    # Koneksi ke PostgreSQL
    conn_source = get_db_connection(DB_SOURCE)
    df = pd.read_sql("SELECT * FROM lead_histories", con=conn_source)
    conn_source.close()

    if df.empty:
        logging.info("✅ Tidak ada data yang diambil, DataFrame kosong!")
        return

    logging.info("✅ Data berhasil diambil!")

    logging.info("🔹 Memproses data...")

    # **Membersihkan data**
    columns_to_remove = ["updated_by", "created_at", "updated_at"]
    df_cleaned = df.drop(columns=columns_to_remove, errors="ignore")

    df_cleaned.fillna({
        "course_package_name": "UNKNOWN",
        "branch_id": 0,
        "utm_campaign": "UNKNOWN",
        "utm_source": "UNKNOWN",
        "utm_medium": "UNKNOWN",
        "utm_content": "UNKNOWN",
        "response_info": "UNKNOWN",
        "lead_status_id": 0,
        "lead_category_id": 0,
        "affiliate": "UNKNOWN",
        "description": "UNKNOWN",
        "source": "UNKNOWN",
        "created_by": "UNKNOWN",
        "deleted_by": "UNKNOWN",
        "registration_id": "UNKNOWN",
        "is_bio_filled": False,
        "bank": "UNKNOWN",
        "cs_group_id": "UNKNOWN",
        "cs_team_id": "UNKNOWN",
        "customer_service_id": "UNKNOWN",
        "is_replied": False,
        "name": "UNKNOWN"
    }, inplace=True)

    # **Konversi Tipe Data**
    datetime_columns = ["created_at", "updated_at", "deleted_at", "registration_date", "payment_date", "period_date"]
    
    for col in datetime_columns:
        if col in df_cleaned.columns:
            df_cleaned[col] = pd.to_datetime(df_cleaned[col], errors="coerce")
            df_cleaned[col] = df_cleaned[col].where(df_cleaned[col].notnull(), None)  # Ganti NaT dengan None

    df_cleaned = df_cleaned.drop_duplicates(subset=["lead_id"])

    logging.info("✅ Data berhasil dibersihkan!")

    # **Koneksi ke MySQL (Target)**
    logging.info("🔹 Memeriksa data yang sudah ada di MySQL...")
    conn_target = get_db_connection(DB_TARGET)
    cursor_target = conn_target.cursor()

    cursor_target.execute("SELECT id FROM lead_histories_cleaned")
    existing_ids = set(row[0] for row in cursor_target.fetchall())

    # 🔹 **Filter hanya data baru**
    df_new = df_cleaned[~df_cleaned["id"].isin(existing_ids)]
    
    logging.info(f"🔍 Data baru yang ditemukan: {len(df_new)} record(s)")

    if not df_new.empty:
        # **Ubah DataFrame ke format tuple**
        cleaned_tuples = [tuple(None if pd.isna(value) else value for value in row) for row in df_new.itertuples(index=False, name=None)]

        sql_insert = """
        INSERT IGNORE INTO lead_histories_cleaned 
        (id, lead_id, course_package_name, branch_id, utm_campaign, utm_source, utm_medium, utm_content, 
        response_info, lead_status_id, lead_category_id, affiliate, description, source, created_at, 
        updated_at, deleted_at, created_by, updated_by, deleted_by, registration_id, registration_date, 
        payment_date, is_bio_filled, bank, period_date, cs_group_id, cs_team_id, customer_service_id, 
        is_replied, name) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor_target.executemany(sql_insert, cleaned_tuples)
        conn_target.commit()
        logging.info(f"✅ {len(df_new)} data baru berhasil dimasukkan ke database MySQL!")
    else:
        logging.info("✅ Tidak ada data baru untuk dimasukkan hari ini.")

    conn_target.close()

    logging.info("✅ Proses ETL selesai!")

# **Fungsi untuk menghitung jumlah data di tabel target**
def count_records():
    conn_target = get_db_connection(DB_TARGET)
    cursor_target = conn_target.cursor()

    cursor_target.execute("SELECT COUNT(*) FROM lead_histories_cleaned")
    total_records = cursor_target.fetchone()[0]

    print(f"✅ Total records in lead_histories_cleaned: {total_records}")
    logging.info(f"✅ Total records in lead_histories_cleaned: {total_records}")

    conn_target.close()
    return total_records

# **Setup DAG**
dag = DAG(
    'etl_lead_histories',
    default_args={'owner': 'airflow', 'start_date': datetime(2025, 1, 29)},
    schedule_interval='@daily',
    catchup=False
)

# **Task ETL**
task_etl = PythonOperator(
    task_id='etl_process',
    python_callable=etl_process,
    dag=dag
)

# **Task Hitung Data**
task_count = PythonOperator(
    task_id='count_records',
    python_callable=count_records,
    dag=dag
)

# **Menjalankan task_count setelah ETL selesai**
task_etl >> task_count
