from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from db import fetch_avance_by_dni
from schemas import AvanceClubPowerResponse
import os

from admin_upload import router as admin_router

app = FastAPI(title="Club Power API")

FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "*")

# Configuración de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_ORIGIN] if FRONTEND_ORIGIN != "*" else ["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*", "X-Admin-Token", "Content-Type"],
)

app.include_router(admin_router)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/avance/{dni}", response_model=AvanceClubPowerResponse)
def get_avance(dni: str):
    if not dni.isdigit() or not (6 <= len(dni) <= 12):
        raise HTTPException(status_code=400, detail="DNI inválido")

    data = fetch_avance_by_dni(dni)
    if not data:
        raise HTTPException(status_code=404, detail="No encontrado")

    return data
