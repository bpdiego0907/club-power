# api/import_puntos.py
from pathlib import Path
import sys
import pandas as pd
from sqlalchemy import text
from dotenv import load_dotenv
from db import engine

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

TABLE_NAME = "club_power_avance"

def normaliza_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza columnas y valida el archivo de avance Club Power.
    Formato esperado (nombres flexibles):
      DNI, NOMBRE, DIA,
      PP TOTAL, PP VR, PORTA PP,
      SS TOTAL, SS VR, OPP, OSS
    """
    # 1) Normalizar nombres de columnas
    df.columns = [c.strip().lower().replace("\n", " ") for c in df.columns]

    # Mapa flexible de nombres (por si vienen con espacios o variaciones)
    mapa = {
        "dni": "dni",
        "documento": "dni",
        "nro dni": "dni",
        "número dni": "dni",

        "nombre": "nombre",
        "nombres": "nombre",
        "apellido y nombre": "nombre",
        "asesor": "nombre",

        "dia": "dia",
        "día": "dia",
        "fecha": "dia",

        "pp total": "pp_total",
        "pptotal": "pp_total",

        "pp vr": "pp_vr",
        "ppvr": "pp_vr",
        "vr pp": "pp_vr",

        "porta pp": "porta_pp",
        "portapp": "porta_pp",
        "porta": "porta_pp",

        "ss total": "ss_total",
        "sstotal": "ss_total",

        "ss vr": "ss_vr",
        "ssvr": "ss_vr",
        "vr ss": "ss_vr",

        "opp": "opp",
        "oss": "oss",
    }

    df = df.rename(columns={c: mapa.get(c, c) for c in df.columns})

    requeridos = ["dni", "nombre", "dia", "pp_vr", "porta_pp", "ss_vr", "opp", "oss"]
    faltantes = [c for c in requeridos if c not in df.columns]
    if faltantes:
        raise ValueError(f"Faltan columnas requeridas: {', '.join(faltantes)}")

    # 2) DNI limpio
    df["dni"] = df["dni"].astype(str).str.strip()
    df = df[df["dni"].str.match(r"^\d{6,12}$", na=False)]

    # 3) Nombre limpio
    df["nombre"] = df["nombre"].fillna("").astype(str).str.strip()

    # 4) Parsear DIA (lo leemos, pero luego lo forzamos a D-1)
    #    Igual lo parseamos por si quieres usarlo para validación/log.
    df["dia"] = pd.to_datetime(df["dia"], errors="coerce").dt.date

    # 5) Numéricos (todo int)
    for c in ["pp_total", "pp_vr", "porta_pp", "ss_total", "ss_vr", "opp", "oss"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
        else:
            # si no vienen totales, se crean
            df[c] = 0

    # 6) Recalcular totales a partir del desglose (garantiza consistencia)
    df["pp_total"] = df["pp_vr"] + df["porta_pp"]
    df["ss_total"] = df["ss_vr"] + df["opp"] + df["oss"]

    # 7) Forzar DIA = D-1 (día cerrado)
    #    Esto evita que el CHECK de la BD reviente.
    df["dia"] = pd.Timestamp.today().normalize().date()  # hoy 00:00
    df["dia"] = pd.to_datetime(df["dia"]) - pd.Timedelta(days=1)
    df["dia"] = df["dia"].dt.date

    # 8) Dedup por DNI (última aparición del archivo)
    df = df.drop_duplicates(subset=["dni"], keep="last").reset_index(drop=True)

    return df


def upsert_chunk(conn, chunk: pd.DataFrame) -> int:
    sql = text(f"""
        INSERT INTO public.{TABLE_NAME}
            (dni, nombre, dia,
             pp_total, pp_vr, porta_pp,
             ss_total, ss_vr, opp, oss,
             updated_at)
        VALUES
            (:dni, :nombre, :dia,
             :pp_total, :pp_vr, :porta_pp,
             :ss_total, :ss_vr, :opp, :oss,
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
    conn.execute(sql, chunk.to_dict(orient="records"))
    return len(chunk)


def main():
    if len(sys.argv) < 2:
        print("Uso: python import_puntos.py <ruta_csv_o_excel> [sep]")
        sys.exit(1)

    file_path = sys.argv[1].strip().strip('"')

    try:
        if file_path.lower().endswith((".xlsx", ".xls")):
            df = pd.read_excel(file_path, dtype=str)
        else:
            sep = sys.argv[2] if len(sys.argv) > 2 else ","
            df = pd.read_csv(file_path, dtype=str, sep=sep)
    except Exception as e:
        print(f"❌ Error al leer el archivo: {e}")
        sys.exit(2)

    try:
        df = normaliza_df(df)
    except Exception as e:
        print(f"❌ Error en normalización/validación: {e}")
        sys.exit(3)

    total = len(df)
    print(f"🧹 Registros tras limpieza/validación: {total:,}")
    if total == 0:
        print("⚠️ No hay registros válidos para procesar.")
        sys.exit(0)

    CHUNK_SIZE = 1000
    procesadas = 0

    try:
        with engine.begin() as conn:
            for i in range(0, total, CHUNK_SIZE):
                ch = df.iloc[i:i + CHUNK_SIZE].copy()
                n = upsert_chunk(conn, ch)
                procesadas += n
                print(f"   → {procesadas:,}/{total:,} filas procesadas...")
    except Exception as e:
        print(f"❌ Error durante el upsert: {e}")
        sys.exit(4)

    print(f"✅ Upsert completado. Filas procesadas: {procesadas:,}")


if __name__ == "__main__":
    main()
