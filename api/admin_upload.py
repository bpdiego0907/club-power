# api/admin_upload.py
import io
import os
import pandas as pd
from fastapi import APIRouter, UploadFile, File, HTTPException, Header
from sqlalchemy import text
from db import engine

router = APIRouter(prefix="/admin", tags=["admin"])

TABLE_NAME = "club_power_avance"

REQUIRED_COLS = [
    "dni","nombre","dia",
    "pp_total","pp_vr","porta_pp",
    "ss_total","ss_vr","opp","oss",
    "meta_ene_pp","meta_ene_ss","meta_feb_pp","meta_feb_ss",
]

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [" ".join(c.strip().lower().split()) for c in df.columns]
    return df

def _validate_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    df = _normalize_columns(df)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas: {', '.join(missing)}")

    # DNI
    df["dni"] = df["dni"].astype(str).str.strip()
    df = df[df["dni"].str.match(r"^\d{6,12}$", na=False)].copy()

    # Nombre
    df["nombre"] = df["nombre"].fillna("").astype(str).str.strip()

    # Dia (igual lo forzamos a D-1)
    df["dia"] = pd.to_datetime(df["dia"], errors="coerce").dt.date

    # Números
    num_cols = [
        "pp_total","pp_vr","porta_pp","ss_total","ss_vr","opp","oss",
        "meta_ene_pp","meta_ene_ss","meta_feb_pp","meta_feb_ss"
    ]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    # Recalcular totales
    df["pp_total"] = df["pp_vr"] + df["porta_pp"]
    df["ss_total"] = df["ss_vr"] + df["opp"] + df["oss"]

    # Forzar D-1 para todos
    d1 = (pd.Timestamp.today().normalize() - pd.Timedelta(days=1)).date()
    df["dia"] = d1

    # Dedup por dni
    df = df.drop_duplicates(subset=["dni"], keep="last").reset_index(drop=True)

    return df

def _upsert_df(conn, df: pd.DataFrame):
    sql = text(f"""
        INSERT INTO public.{TABLE_NAME}
            (dni, nombre, dia,
             pp_total, pp_vr, porta_pp,
             ss_total, ss_vr, opp, oss,
             meta_ene_pp, meta_ene_ss, meta_feb_pp, meta_feb_ss,
             created_at, updated_at)
        VALUES
            (:dni, :nombre, :dia,
             :pp_total, :pp_vr, :porta_pp,
             :ss_total, :ss_vr, :opp, :oss,
             :meta_ene_pp, :meta_ene_ss, :meta_feb_pp, :meta_feb_ss,
             now(), now())
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
    conn.execute(sql, df.to_dict(orient="records"))

@router.post("/cargar-base")
async def cargar_base(
    file: UploadFile = File(...),
    x_admin_token: str | None = Header(default=None),
):
    # 🔒 Seguridad mínima por token
    ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=500, detail="ADMIN_TOKEN no configurado en el servidor.")
    if x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="No autorizado.")

    filename = (file.filename or "").lower()
    content = await file.read()

    try:
        if filename.endswith((".xlsx", ".xls")):
            df = pd.read_excel(io.BytesIO(content), dtype=str)
        elif filename.endswith(".csv"):
            # Tu CSV es separado por comas
            df = pd.read_csv(io.BytesIO(content), dtype=str, sep=",")
        else:
            raise HTTPException(status_code=400, detail="Formato no soportado. Sube .csv o .xlsx")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"No se pudo leer el archivo: {e}")

    try:
        df = _validate_and_clean(df)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    if len(df) == 0:
        raise HTTPException(status_code=400, detail="El archivo no tiene filas válidas.")

    # ✅ TRUNCATE + UPSERT en una sola transacción
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE public.{TABLE_NAME} RESTART IDENTITY;"))
        _upsert_df(conn, df)

    return {"ok": True, "filas_cargadas": int(len(df)), "dia_forzado": str(df["dia"].iloc[0])}
