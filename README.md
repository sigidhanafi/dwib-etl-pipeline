# Data Warehouse untuk Perbankan (Setup, ETL, Airflow DAG)

## Struktur Folder

- `etl/` – Script ETL untuk masing-masing tabel
- `dags/` – Script DAG untuk proses pada airflow
- `db/` – File database DuckDB
- `data/` – File data mentah (.csv)
- `sql/` – Script SQL untuk proses setup table dimensi dan table fact
- `scripts/` – Script setup dan utility
- `config/`, `plugins/`, `logs/`  – Directory default dari airflow
- `requirements.txt` – Daftar dependensi Python
- `docker-compose.yaml` - Docker untuk menjalankan airflow

## Setup Database & ETL Table

1. Install environment:

```bash
 pip install -r requirements.txt
```

2. How to run setup database and ETL process?

```bash
 python3 main.py
```
Script ini digunakan untuk membuat database dan table untuk kebutuhan proses ETL. Pastikan database dan table sudah terbuat dahulu sebelum menjalankan proses ETL dengan menggunakan DAG pada Airflow

## Menjalankan Airflow

1. jalankan docker & environment pada server / local machine

```bash
 docker-compose up -d
```

2. open http://localhost:8080
3. login airflow
4. DAG akan muncul pada menu DAGs (proses DAG dijelaskan pada attachment Orkestrasi Data Warehouse)


## Attachments
Definisi:
1. Dokumen Kebutuhan Bisnis [Laporan.pdf](https://github.com/sigidhanafi/dwib-etl-pipeline/blob/main/Lampiran/Laporan.pdf)
2. [Diagram Skema Data Warehouse](https://github.com/sigidhanafi/dwib-etl-pipeline/blob/main/Lampiran/Data%20Warehouse%20Schema.png)
3. Deskripsi Dataset - [Data Warehouse & ETL](https://github.com/sigidhanafi/dwib-etl-pipeline/blob/main/Lampiran/Data%20Warehouse%20and%20ETL.pdf)
4. Script ETL Python/pandas (repository)
5. [Orkestrasi Data Warehouse](https://github.com/sigidhanafi/dwib-etl-pipeline/blob/main/Lampiran/Data%20Orchestration%20Apache%20Airflow.pdf)



## Note: If you run with Python VENV:
```bash
git clone git@github.com:sigidhanafi/dwib-etl-pipeline.git
cd dwib-etl-pipeline
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
