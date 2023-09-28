from pydantic import BaseModel
from core.config import settings


class CsrfSettings(BaseModel):
    secret_key: str = settings.CSRF