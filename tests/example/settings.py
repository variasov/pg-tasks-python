from pydantic import PostgresDsn
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DB_URL: PostgresDsn
    NUMBER_OF_WORKERS: int = 1
    DB_POOL_SIZE: int = 1

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
