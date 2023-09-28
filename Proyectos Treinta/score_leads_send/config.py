from pydantic import BaseSettings

class Settings(BaseSettings):

    TYPE: str
    PROJECT_ID: str
    PRIVATE_KEY_ID: str
    PRIVATE_KEY: str
    CLIENT_EMAIL: str
    CLIENT_ID: str
    AUTH_URI: str
    TOKEN_URI: str
    AUTH_PROVIDER: str 
    CLIENT_URL: str
    
    class Config:
        env_file = ".env"
 
settings = Settings()