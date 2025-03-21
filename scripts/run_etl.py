from etl.dim_channel import etl_dim_channel
from etl.dim_customer import etl_dim_customer
from etl.dim_device import etl_dim_device
from etl.dim_location import etl_dim_location
from etl.dim_time import etl_dim_time
from etl.dim_type import etl_dim_type
from etl.fact_transaction import etl_fact_transaction

import pandas as pd
import duckdb
import os

def run_etl():
  print(f"Proses ETL:")
  print("\tMemulai proses ETL!")
  con = None

  try:
    # Lokasi file CSV relatif terhadap script ini
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(base_dir, "..", "data", "transactions.csv")
    data_path = os.path.normpath(data_path)
    df = pd.read_csv(data_path)
    print("\t✅Proses extract data berhasil!")

    con = duckdb.connect("db/dwh-perbankan.duckdb")
    print("\t✅Koneksi ke database berhasil!")

    # etl_dim_customer(df, con)
    # etl_dim_time(df, con)
    # etl_dim_location(df, con)
    # etl_dim_device(df, con)
    etl_dim_channel(df, con)
    # etl_dim_type(df, con)
    # etl_fact_transaction(df, con)

  except FileNotFoundError:
      print(f"\t\t❌File transaksi tidak ditemukan di path: {data_path}")
      return False

  except duckdb.Error as e:
      print(f"\t\t❌Gagal koneksi atau query DuckDB: {e}")
      return False

  except Exception as e:
      print(f"\t\t❌Terjadi error tak terduga di ETL: {e}")
      return False

  finally:
      if con:
            # Tutup koneksi
            con.close()
            print("\tKoneksi database ditutup!")

      print("\tProses ETL Selesai!")

  return True