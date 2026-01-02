from pydantic import BaseModel, Field
from datetime import date, datetime

class AvanceClubPowerResponse(BaseModel):
    dni: str = Field(..., example="666666")
    nombre: str = Field(..., example="Juan Pérez")
    dia: date = Field(..., example="2026-01-05")

    # PP
    pp_total: int = Field(..., example=56)
    pp_vr: int = Field(..., example=40)
    porta_pp: int = Field(..., example=16)

    # SS
    ss_total: int = Field(..., example=3)
    ss_vr: int = Field(..., example=1)
    opp: int = Field(..., example=1)
    oss: int = Field(..., example=1)

    updated_at: datetime = Field(..., example="2026-01-06T07:30:12")
