from scripts.setup import setup
from scripts.run_etl import run_etl

if __name__ == "__main__":
  try:
      # logging.info("Memulai proses setup.")
      success = setup()
      if not success:
          raise ValueError("Terjadi error menjalankan setup() function")
      
      success = run_etl()
      if not success:
          raise ValueError("Terjadi error saat menjalankan run_etl() function")

  except Exception as e:
      print(f"\n❌{e}")

  finally:
    print("\nProgram Selesai")
    print("\n")