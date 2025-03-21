# Data Warehouse Perbankan

## Struktur Folder

- `etl/` – Script ETL untuk masing-masing tabel
- `db/` – File database DuckDB
- `data/` – File data mentah (.csv)
- `scripts/` – Script setup dan utility
- `requirements.txt` – Daftar dependensi Python

## Menjalankan Proyek

1. Install environment:

```bash
 pip install -r requirements.txt
```

2. How to run settup and ETL process?

```bash
 python3 main.py
```



Note: If you run with Python VENV, 
1. cd to project directory
2. source dwh-env/bin/activate
