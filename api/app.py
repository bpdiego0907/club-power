from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from db import fetch_avance_by_dni
from schemas import AvanceClubPowerResponse
import os

app = FastAPI(title='Club Power API')

# Configuración de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[os.getenv('FRONTEND_ORIGIN', '*')],
    allow_credentials=True,
    allow_methods=['GET'],
    allow_headers=['*'],
)

@app.get('/health')
def health():
    return {'status': 'ok'}

@app.get('/avance/{dni}', response_model=AvanceClubPowerResponse)
def get_avance(dni: str):
    # Validación básica de DNI
    if not dni.isdigit() or not (6 <= len(dni) <= 12):
        raise HTTPException(status_code=400, detail='DNI inválido')

    data = fetch_avance_by_dni(dni)

    if not data:
        raise HTTPException(status_code=404, detail='No encontrado')

    return data
