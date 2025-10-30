import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

# Cargar .env desde el cwd o junto al archivo
env_path = find_dotenv(filename=".env", usecwd=True)
load_dotenv(dotenv_path=env_path)

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise RuntimeError(
        "DB_URL no está definido. Verifica tu archivo .env en la carpeta 'api' "
        "o exporta la variable de entorno antes de iniciar Uvicorn."
    )

engine = create_engine(
    DB_URL,
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=5,
    pool_pre_ping=True,
)

def fetch_premios_by_dni(dni: str):
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT dni,
                   puntos_1er_premio AS canasta,
                   puntos_2do_premio AS pavo
            FROM club_power_puntos
            WHERE dni = :dni
        """), {"dni": dni}).mappings().first()
        return dict(row) if row else None
