# api/migrate_add_metas.py
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import text
from db import engine

# Cargar .env
load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

TABLE_NAME = "club_power_avance"

def main():
    sql = text(f"""
        ALTER TABLE public.{TABLE_NAME}
          ADD COLUMN IF NOT EXISTS meta_ene_pp INTEGER NOT NULL DEFAULT 0,
          ADD COLUMN IF NOT EXISTS meta_ene_ss INTEGER NOT NULL DEFAULT 0,
          ADD COLUMN IF NOT EXISTS meta_feb_pp INTEGER NOT NULL DEFAULT 0,
          ADD COLUMN IF NOT EXISTS meta_feb_ss INTEGER NOT NULL DEFAULT 0;
    """)

    with engine.begin() as conn:
        print(f"▶️ Migrando tabla {TABLE_NAME}...")
        conn.execute(sql)
        print("✅ Columnas de metas creadas/verificadas con éxito.")

if __name__ == "__main__":
    main()