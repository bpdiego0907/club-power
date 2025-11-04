from pydantic import BaseModel, Field

class PremiosResponse(BaseModel):
    dni: str = Field(..., example="47178389")
    canasta: int = Field(..., example=146)
    pavo: int = Field(..., example=168)
    puntos: float = Field(0.0, example=0.0)
    pv: str = Field("", example="PV47178389")
