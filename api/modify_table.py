import os
import psycopg
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DB_URL", "")
if not DB_URL:
    raise RuntimeError("DB_URL no está definido")

DB_URL = DB_URL.replace("postgresql+psycopg://", "postgresql://")

TABLE = "public.club_power_avance"

RENAMES = [
    ("meta_ene_pp", "meta_mar_pp"),
    ("meta_ene_ss", "meta_mar_ss"),
    ("meta_feb_pp", "meta_abr_pp"),
    ("meta_feb_ss", "meta_abr_ss"),
]

SQL_COL_EXISTS = """
SELECT 1
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'club_power_avance'
  AND column_name = %s
LIMIT 1;
"""

def col_exists(cur, col_name: str) -> bool:
    cur.execute(SQL_COL_EXISTS, (col_name,))
    return cur.fetchone() is not None

def rename_column(cur, old: str, new: str):
    # Si ya existe la nueva, no hacemos nada
    if col_exists(cur, new):
        print(f"Ya existe {new}. Saltando rename {old} -> {new}")
        return

    # Si no existe la vieja, avisamos
    if not col_exists(cur, old):
        print(f"No existe {old}. Nada que renombrar.")
        return

    cur.execute(f'ALTER TABLE {TABLE} RENAME COLUMN "{old}" TO "{new}";')
    print(f"Renombrada: {old} -> {new}")

def main():
    print("Conectando a la base…")
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            for old, new in RENAMES:
                rename_column(cur, old, new)
        conn.commit()
    print("Listo. Renombres aplicados.")

if __name__ == "__main__":
    main()