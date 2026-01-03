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

    CSV esperado (ya estandarizado en tu caso):
      dni, nombre, dia,
      pp_total, pp_vr, porta_pp,
      ss_total, ss_vr, opp, oss,
      meta_ene_pp, meta_ene_ss, meta_feb_pp, meta_feb_ss

    Reglas:
    - Se recalculan pp_total y ss_total desde el desglose.
    - Se fuerza dia = D-1 (día cerrado) para TODAS las filas.
    - Se hace dedup por dni (última aparición).
    """

    # 1) Normalizar nombres de columnas (robusto: colapsa espacios)
    df.columns = [" ".join(c.strip().lower().replace("\n", " ").split()) for c in df.columns]

    # 2) Mapa flexible por si en algún momento vuelven a venir con espacios/variantes
    mapa = {
        # DNI / nombre / fecha
        "dni": "dni",
        "documento": "dni",
        "nro dni": "dni",
        "número dni": "dni",
        "numero dni": "dni",

        "nombre": "nombre",
        "nombres": "nombre",
        "apellido y nombre": "nombre",
        "asesor": "nombre",

        "dia": "dia",
        "día": "dia",
        "fecha": "dia",

        # Prepago
        "pp_total": "pp_total",
        "pp total": "pp_total",
        "pptotal": "pp_total",

        "pp_vr": "pp_vr",
        "pp vr": "pp_vr",
        "ppvr": "pp_vr",
        "vr pp": "pp_vr",

        "porta_pp": "porta_pp",
        "porta pp": "porta_pp",
        "portapp": "porta_pp",
        "porta": "porta_pp",

        # Postpago
        "ss_total": "ss_total",
        "ss total": "ss_total",
        "sstotal": "ss_total",

        "ss_vr": "ss_vr",
        "ss vr": "ss_vr",
        "ssvr": "ss_vr",
        "vr ss": "ss_vr",

        "opp": "opp",
        "oss": "oss",

        # Metas
        "meta_ene_pp": "meta_ene_pp",
        "meta_ene_ss": "meta_ene_ss",
        "meta_feb_pp": "meta_feb_pp",
        "meta_feb_ss": "meta_feb_ss",
        # por si vienen con espacios
        "meta ene pp": "meta_ene_pp",
        "meta ene ss": "meta_ene_ss",
        "meta feb pp": "meta_feb_pp",
        "meta feb ss": "meta_feb_ss",
    }

    df = df.rename(columns={c: mapa.get(c, c) for c in df.columns})

    # 3) Validación de columnas mínimas
    requeridos = [
        "dni",
        "nombre",
        "dia",
        "pp_vr",
        "porta_pp",
        "ss_vr",
        "opp",
        "oss",
        "meta_ene_pp",
        "meta_ene_ss",
        "meta_feb_pp",
        "meta_feb_ss",
    ]
    faltantes = [c for c in requeridos if c not in df.columns]
    if faltantes:
        raise ValueError(f"Faltan columnas requeridas: {', '.join(faltantes)}")

    # 4) DNI limpio y válido
    df["dni"] = df["dni"].astype(str).str.strip()
    df = df[df["dni"].str.match(r"^\d{6,12}$", na=False)].copy()

    # 5) Nombre limpio
    df["nombre"] = df["nombre"].fillna("").astype(str).str.strip()

    # 6) Parsear DIA (se lee pero luego se fuerza D-1)
    df["dia"] = pd.to_datetime(df["dia"], errors="coerce").dt.date

    # 7) Numéricos (int) — si faltaran totales, igual se crean
    for c in ["pp_total", "pp_vr", "porta_pp", "ss_total", "ss_vr", "opp", "oss"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
        else:
            df[c] = 0

    # 8) Metas a int
    for c in ["meta_ene_pp", "meta_ene_ss", "meta_feb_pp", "meta_feb_ss"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    # 9) Recalcular totales desde desglose (consistencia)
    df["pp_total"] = df["pp_vr"] + df["porta_pp"]
    df["ss_total"] = df["ss_vr"] + df["opp"] + df["oss"]

    # 10) Forzar DIA = D-1 (día cerrado) para TODAS las filas
    d1 = (pd.Timestamp.today().normalize() - pd.Timedelta(days=1)).date()
    df["dia"] = d1

    # 11) Dedup por DNI (última aparición del archivo)
    df = df.drop_duplicates(subset=["dni"], keep="last").reset_index(drop=True)

    return df


def upsert_chunk(conn, chunk: pd.DataFrame) -> int:
    sql = text(f"""
        INSERT INTO public.{TABLE_NAME}
            (dni, nombre, dia,
             pp_total, pp_vr, porta_pp,
             ss_total, ss_vr, opp, oss,
             meta_ene_pp, meta_ene_ss, meta_feb_pp, meta_feb_ss,
             updated_at)
        VALUES
            (:dni, :nombre, :dia,
             :pp_total, :pp_vr, :porta_pp,
             :ss_total, :ss_vr, :opp, :oss,
             :meta_ene_pp, :meta_ene_ss, :meta_feb_pp, :meta_feb_ss,
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
            meta_ene_pp = EXCLUDED.meta_ene_pp,
            meta_ene_ss = EXCLUDED.meta_ene_ss,
            meta_feb_pp = EXCLUDED.meta_feb_pp,
            meta_feb_ss = EXCLUDED.meta_feb_ss,
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
