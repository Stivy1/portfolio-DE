from pydantic import BaseSettings

class Settings(BaseSettings):

    GOOGLE_APPLICATION_CREDENTIALS: str
    
    class Config:
        env_file = ".env"
 
settings = Settings()