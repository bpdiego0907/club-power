# api/truncate_table.py
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path
import os

# Cargar variables de entorno
load_dotenv(Path(__file__).parent / ".env")

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise RuntimeError("DB_URL no está definido")

engine = create_engine(DB_URL.replace("postgresql://", "postgresql+psycopg://", 1))

TABLE_NAME = "club_power_avance"

with engine.begin() as conn:
    conn.execute(text(f"TRUNCATE TABLE public.{TABLE_NAME} RESTART IDENTITY;"))
    print(f"✅ Tabla {TABLE_NAME} truncada con éxito.")
