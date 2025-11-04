from pydantic import BaseModel, Field

class PremiosResponse(BaseModel):
    dni: str = Field(..., example="47178389")
    canasta: int = Field(..., example=146)
    pavo: int = Field(..., example=168)
    puntos: int = Field(0, example=0)
    pv: str = Field("", example="PV47178389")