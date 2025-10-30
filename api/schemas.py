from pydantic import BaseModel, Field

class PremiosResponse(BaseModel):
    dni: str = Field(..., example='666666')
    canasta: int = Field(..., example=300)
    pavo: int = Field(..., example=400)
