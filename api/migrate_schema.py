# api/migrate_schema.py
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import text
from db import engine

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

TABLE = "club_power_avance"

TARGET = {
    "dni_len": 20,
    "nombre_len": 120,
}

def col_info(conn, table, column):
    q = text("""
        SELECT data_type,
               udt_name,
               character_maximum_length AS char_len,
               numeric_precision,
               numeric_scale,
               is_nullable,
               column_default
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = :t
          AND column_name = :c
        LIMIT 1
    """)
    return conn.execute(q, {"t": table, "c": column}).mappings().first()

def ensure_table(conn):
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS public.{TABLE} (
            id BIGSERIAL PRIMARY KEY,
            dni VARCHAR({TARGET["dni_len"]}) NOT NULL,
            nombre VARCHAR({TARGET["nombre_len"]}) NOT NULL,
            dia DATE NOT NULL,

            pp_total INTEGER NOT NULL DEFAULT 0,
            pp_vr INTEGER NOT NULL DEFAULT 0,
            porta_pp INTEGER NOT NULL DEFAULT 0,

            ss_total INTEGER NOT NULL DEFAULT 0,
            ss_vr INTEGER NOT NULL DEFAULT 0,
            opp INTEGER NOT NULL DEFAULT 0,
            oss INTEGER NOT NULL DEFAULT 0,

            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """))

def ensure_column(conn, name, ddl):
    info = col_info(conn, TABLE, name)
    if not info:
        conn.execute(text(f"ALTER TABLE public.{TABLE} ADD COLUMN {name} {ddl};"))
        print(f"➕ Columna creada: {name} {ddl}")

def widen_varchar(conn, name, min_len):
    info = col_info(conn, TABLE, name)
    if not info:
        return
    if info["data_type"] in ("character varying", "varchar"):
        cur_len = info["char_len"] or 0
        if cur_len < min_len:
            conn.execute(text(f"""
                ALTER TABLE public.{TABLE}
                ALTER COLUMN {name} TYPE VARCHAR({min_len});
            """))
            print(f"🔁 {name} ensanchada a VARCHAR({min_len})")

def ensure_not_null_default(conn, col, default_sql=None):
    info = col_info(conn, TABLE, col)
    if not info:
        return
    if info["is_nullable"] == "YES":
        # poner valores nulos a default antes de NOT NULL
        if default_sql is not None:
            conn.execute(text(f"UPDATE public.{TABLE} SET {col} = {default_sql} WHERE {col} IS NULL;"))
        else:
            conn.execute(text(f"UPDATE public.{TABLE} SET {col} = 0 WHERE {col} IS NULL;"))
        conn.execute(text(f"ALTER TABLE public.{TABLE} ALTER COLUMN {col} SET NOT NULL;"))
        print(f"🔒 {col} SET NOT NULL")

    if default_sql is not None:
        # set default si no coincide
        conn.execute(text(f"ALTER TABLE public.{TABLE} ALTER COLUMN {col} SET DEFAULT {default_sql};"))

def ensure_constraint(conn, ddl):
    # ddl debe ser un ALTER TABLE ... ADD CONSTRAINT ...;
    # lo envolvemos para que sea idempotente
    conn.execute(text(f"""
        DO $$
        BEGIN
            {ddl}
        EXCEPTION
            WHEN duplicate_object THEN
                NULL;
        END $$;
    """))

def ensure_index(conn, name, ddl):
    # ddl: CREATE INDEX IF NOT EXISTS ...
    conn.execute(text(ddl))

def main():
    with engine.begin() as conn:
        print(f"▶️ Migrando esquema {TABLE}…")

        ensure_table(conn)

        # Columnas mínimas (por si la tabla existía a medias)
        ensure_column(conn, "dni", f"VARCHAR({TARGET['dni_len']}) NOT NULL")
        ensure_column(conn, "nombre", f"VARCHAR({TARGET['nombre_len']}) NOT NULL")
        ensure_column(conn, "dia", "DATE NOT NULL")

        for c in ["pp_total", "pp_vr", "porta_pp", "ss_total", "ss_vr", "opp", "oss"]:
            ensure_column(conn, c, "INTEGER NOT NULL DEFAULT 0")

        ensure_column(conn, "created_at", "TIMESTAMPTZ NOT NULL DEFAULT now()")
        ensure_column(conn, "updated_at", "TIMESTAMPTZ NOT NULL DEFAULT now()")

        # Asegurar largos
        widen_varchar(conn, "dni", TARGET["dni_len"])
        widen_varchar(conn, "nombre", TARGET["nombre_len"])

        # Asegurar NOT NULL + DEFAULTS en contadores
        for c in ["pp_total", "pp_vr", "porta_pp", "ss_total", "ss_vr", "opp", "oss"]:
            ensure_not_null_default(conn, c, "0")

        # Constraints (idempotentes)
        ensure_constraint(conn, f"""
            ALTER TABLE public.{TABLE}
            ADD CONSTRAINT uk_dni UNIQUE (dni);
        """)

        ensure_constraint(conn, f"""
            ALTER TABLE public.{TABLE}
            ADD CONSTRAINT ck_pp_total CHECK (pp_total = pp_vr + porta_pp);
        """)

        ensure_constraint(conn, f"""
            ALTER TABLE public.{TABLE}
            ADD CONSTRAINT ck_ss_total CHECK (ss_total = ss_vr + opp + oss);
        """)

        ensure_constraint(conn, f"""
            ALTER TABLE public.{TABLE}
            ADD CONSTRAINT ck_dia_d_menos_1 CHECK (dia = CURRENT_DATE - 1);
        """)

        # Índice útil (además del UNIQUE)
        ensure_index(conn, "ix_club_power_avance_dni",
                     f"CREATE INDEX IF NOT EXISTS ix_club_power_avance_dni ON public.{TABLE} (dni);")

        print("✅ Migración completada.")

if __name__ == "__main__":
    main()
