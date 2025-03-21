import duckdb
from pathlib import Path


def setup():
    print(f"Setup:")
    print(f"\tMemulai proses setup database & table in DuckDB!")

    con = None

    try:
        # Koneksi ke database DuckDB (file-based)
        con = duckdb.connect("db/dwh-perbankan.duckdb")

        # Folder berisi file SQL
        sql_dir = Path("sql")

        for file in sorted(sql_dir.glob("*.sql")):
            print(f"\tMenjalankan {file.name}...")
            ddl_script = file.read_text()
            con.execute(ddl_script)

        print("\t✅ DDL Script sukses dijalankan!")

        # tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchdf()

        # # Looping untuk menampilkan struktur semua tabel
        # for table in tables["table_name"]:
        #     print(f"\n ==== Tabel: {table} ====")
        #     desc = con.execute(f"DESCRIBE {table}").fetchdf()
        #     print(desc)


        # # Tampilkan daftar tabel
        # print(tables)

    except duckdb.Error as e:
        print(f"\t❌{e}")
        # logging.error(f"Error DuckDB: {e}")
        # logging.error(traceback.format_exc())
        return False

    except Exception as e:
        print(f"\t❌Error tak terduga: {e}")
        # logging.error(f"Error tak terduga: {e}")
        # logging.error(traceback.format_exc())
        return False

    finally:
        if con:
            # Tutup koneksi
            con.close()
            print("\tKoneksi database ditutup!")

        print("\tSetup database & table Selesai!")
    
    
    return True
