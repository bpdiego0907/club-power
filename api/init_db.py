# -------------------------------------------------------------
# init_db.py — crea/ajusta la tabla y llena datos de prueba
# -------------------------------------------------------------
import os
import psycopg
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DB_URL", "")
if not DB_URL:
    raise RuntimeError("DB_URL no está definido")
DB_URL = DB_URL.replace("postgresql+psycopg://", "postgresql://")

DDL_CREATE = """
CREATE TABLE IF NOT EXISTS public.club_power_puntos (
    id BIGSERIAL PRIMARY KEY,
    dni VARCHAR(20) NOT NULL,
    tipo VARCHAR(50),
    canal VARCHAR(50),
    puntos_1er_premio INTEGER NOT NULL DEFAULT 0,
    puntos_2do_premio INTEGER NOT NULL DEFAULT 0,
    puntos NUMERIC(10,2) NOT NULL DEFAULT 0,
    pv VARCHAR(50),
    CONSTRAINT uk_dni UNIQUE (dni)
);
"""

SQL_COL_INFO = """
SELECT data_type, character_maximum_length AS char_len,
       numeric_precision, numeric_scale
FROM information_schema.columns
WHERE table_schema='public' AND table_name='club_power_puntos' AND column_name=%s
LIMIT 1;
"""

def ensure_varchar_len(cur, col, min_len):
    cur.execute(SQL_COL_INFO, (col,))
    r = cur.fetchone()
    if not r:
        # si no existe la columna, créala
        cur.execute(f"ALTER TABLE public.club_power_puntos ADD COLUMN {col} VARCHAR({min_len});")
        return
    data_type, char_len, _, _ = r
    if data_type in ("character varying", "varchar"):
        if char_len is None or char_len < min_len:
            cur.execute(f"ALTER TABLE public.club_power_puntos ALTER COLUMN {col} TYPE VARCHAR({min_len});")

def ensure_puntos_numeric(cur):
    cur.execute(SQL_COL_INFO, ("puntos",))
    r = cur.fetchone()
    if not r:
        cur.execute("ALTER TABLE public.club_power_puntos ADD COLUMN puntos NUMERIC(10,2) NOT NULL DEFAULT 0;")
        return
    data_type, _, prec, scale = r
    is_numeric = (data_type == "numeric")
    correct = (prec == 10 and scale == 2)
    if (not is_numeric) or (not correct):
        # convertir con USING seguro desde int/text/float → numeric(10,2)
        cur.execute("""
            ALTER TABLE public.club_power_puntos
            ALTER COLUMN puntos TYPE NUMERIC(10,2)
            USING NULLIF(TRIM(puntos::text), '')::numeric(10,2),
            ALTER COLUMN puntos SET DEFAULT 0,
            ALTER COLUMN puntos SET NOT NULL;
        """)

def main():
    print("Conectando a la base…")
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            # 1) Crear tabla si no existe
            cur.execute(DDL_CREATE)

            # 2) Asegurar tipos/largos deseados
            ensure_varchar_len(cur, "tipo", 50)
            ensure_varchar_len(cur, "canal", 50)
            ensure_varchar_len(cur, "pv", 50)
            ensure_puntos_numeric(cur)

            # 3) Datos de prueba (incluye decimales en PUNTOS)
            cur.execute("""
                INSERT INTO public.club_power_puntos
                    (dni, tipo, canal, puntos_1er_premio, puntos_2do_premio, puntos, pv)
                VALUES
                    ('666666','LIDER','PDV',300,400,120.0,'PV-LIMA-01'),
                    ('123455','EQUIPO','MULTIMARCA',200,220,50.5,'PV-AREQUIPA-02'),
                    ('12324356','EQUIPO','FULL PREPAGO',234,257,180.0,'PV-CHICLAYO-03'),
                    ('324355','LIDER','PDV PLUS',346,381,310.0,'PV-LIMA-04'),
                    ('435467','EQUIPO','HC EMO',600,660,440.5,'PV-CUSCO-05')
                ON CONFLICT (dni) DO NOTHING;
            """)
        conn.commit()
    print("✅ Esquema verificado y datos de prueba listos.")

if __name__ == "__main__":
    main()
