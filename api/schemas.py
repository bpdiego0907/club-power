from pydantic import BaseModel, Field
from datetime import date, datetime

class AvanceClubPowerResponse(BaseModel):
    # Identificación
    dni: str = Field(..., example="666666")
    nombre: str = Field(..., example="Juan Pérez")
    dia: date = Field(..., example="2026-01-02")  # D-1

    # ======================
    # AVANCE PP
    # ======================
    pp_total: int = Field(..., example=56)
    pp_vr: int = Field(..., example=40)
    porta_pp: int = Field(..., example=16)

    # ======================
    # AVANCE SS
    # ======================
    ss_total: int = Field(..., example=3)
    ss_vr: int = Field(..., example=1)
    opp: int = Field(..., example=1)
    oss: int = Field(..., example=1)

    # ======================
    # METAS (desde BD)
    # ======================
    meta_ene_pp: int = Field(..., example=50)
    meta_ene_ss: int = Field(..., example=2)
    meta_feb_pp: int = Field(..., example=48)
    meta_feb_ss: int = Field(..., example=1)

    # Auditoría
    updated_at: datetime = Field(..., example="2026-01-06T07:30:12")
