# db.py
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Cargar variables del entorno local (.env)
load_dotenv()

# Leer la URL de conexión
DB_URL = os.getenv("DB_URL")

if not DB_URL:
    raise RuntimeError("DB_URL no está definido. Verifica tu archivo .env o las Variables en Railway.")

# 🔹 Ajustar el driver para usar psycopg (no psycopg2)
SQLA_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)

# Crear el motor de conexión
engine = create_engine(
    SQLA_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=5,
    pool_recycle=1800,
)

# -------------------------------------------------------------
# Función para obtener los premios por DNI
# -------------------------------------------------------------
def fetch_premios_by_dni(dni: str):
    """
    Retorna los premios asociados a un DNI desde la tabla club_power_puntos.
    """
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT dni,
                   puntos_1er_premio AS canasta,
                   puntos_2do_premio AS pavo
            FROM club_power_puntos
            WHERE dni = :dni
        """), {"dni": dni}).mappings().first()
        return row
