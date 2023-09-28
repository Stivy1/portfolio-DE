from pydantic import BaseSettings

class Settings(BaseSettings):

    LOGYCA_LOGIN_URL: str
    LOGYCA_USER: str
    LOGYCA_PASSWORD: str
    LOGYCA_GTINS_URL: str

    MONGO_URI: str
    MONGO_DATABASE: str

    TABLE_ID: str
    PROJECT_ID: str

    CSRF: str

    TYPE: str
    PROJECT_ID_DEV: str
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