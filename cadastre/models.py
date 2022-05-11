from pydantic import BaseModel


class Message(BaseModel):
    message: str


class ParcellCeleryResult(BaseModel):
    input_filename: str
    output_filename: str
    success_rate: float
    failure_rate: float
