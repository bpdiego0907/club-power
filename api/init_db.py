# -------------------------------------------------------------
# init_db.py — crea/ajusta la tabla de avance CLUB POWER
# -------------------------------------------------------------
import os
import psycopg
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DB_URL", "")
if not DB_URL:
    raise RuntimeError("DB_URL no está definido")

DB_URL = DB_URL.replace("postgresql+psycopg://", "postgresql://")

TABLE_NAME = "club_power_avance"

DDL_CREATE = f"""
CREATE TABLE IF NOT EXISTS public.{TABLE_NAME} (
    id BIGSERIAL PRIMARY KEY,
    dni VARCHAR(20) NOT NULL,
    nombre VARCHAR(120) NOT NULL,
    dia DATE NOT NULL,

    pp_total INTEGER NOT NULL DEFAULT 0,
    pp_vr INTEGER NOT NULL DEFAULT 0,
    porta_pp INTEGER NOT NULL DEFAULT 0,

    ss_total INTEGER NOT NULL DEFAULT 0,
    ss_vr INTEGER NOT NULL DEFAULT 0,
    opp INTEGER NOT NULL DEFAULT 0,
    oss INTEGER NOT NULL DEFAULT 0,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uk_dni UNIQUE (dni),
    CONSTRAINT ck_pp_total CHECK (pp_total = pp_vr + porta_pp),
    CONSTRAINT ck_ss_total CHECK (ss_total = ss_vr + opp + oss),
    CONSTRAINT ck_dia_d_menos_1 CHECK (dia = CURRENT_DATE - 1)
);
"""

def main():
    print("Conectando a la base…")
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            # 1) Crear tabla
            cur.execute(DDL_CREATE)

            # 2) Datos de prueba (snapshot D-1)
            cur.execute(f"""
                INSERT INTO public.{TABLE_NAME}
                (dni, nombre, dia,
                 pp_total, pp_vr, porta_pp,
                 ss_total, ss_vr, opp, oss,
                 updated_at)
                VALUES
                ('666666', 'Juan Pérez', CURRENT_DATE - 1,
                 56, 40, 16,
                 3, 1, 1, 1,
                 now()),

                ('123455', 'María López', CURRENT_DATE - 1,
                 45, 30, 15,
                 2, 1, 1, 0,
                 now()),

                ('324355', 'Carlos Ramírez', CURRENT_DATE - 1,
                 62, 50, 12,
                 4, 2, 1, 1,
                 now())
                ON CONFLICT (dni) DO UPDATE SET
                    nombre = EXCLUDED.nombre,
                    dia = EXCLUDED.dia,
                    pp_total = EXCLUDED.pp_total,
                    pp_vr = EXCLUDED.pp_vr,
                    porta_pp = EXCLUDED.porta_pp,
                    ss_total = EXCLUDED.ss_total,
                    ss_vr = EXCLUDED.ss_vr,
                    opp = EXCLUDED.opp,
                    oss = EXCLUDED.oss,
                    updated_at = now();
            """)

        conn.commit()

    print("✅ Tabla CLUB POWER lista y datos de prueba cargados.")

if __name__ == "__main__":
    main()
