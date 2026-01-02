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

# Ajustar el driver para usar psycopg
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
# Función para obtener el avance CLUB POWER por DNI
# -------------------------------------------------------------
def fetch_avance_by_dni(dni: str):
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT
                    dni,
                    nombre,
                    dia,

                    pp_total,
                    pp_vr,
                    porta_pp,

                    ss_total,
                    ss_vr,
                    opp,
                    oss,

                    updated_at
                FROM club_power_avance
                WHERE dni = :dni
            """),
            {"dni": dni}
        ).mappings().first()

        if not row:
            return None

        # Convertir a dict limpio
        out = dict(row)

        # Asegurar tipos (por seguridad)
        for k in [
            "pp_total", "pp_vr", "porta_pp",
            "ss_total", "ss_vr", "opp", "oss"
        ]:
            out[k] = int(out.get(k) or 0)

        return out
