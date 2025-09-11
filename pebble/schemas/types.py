# Placeholders for Pydantic models
from pydantic import BaseModel


class NightQA(BaseModel):
    night_id: str
    mythic_pre_minutes: int
    mythic_post_minutes: int
