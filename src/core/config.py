import logging
from pydantic_settings import BaseSettings

class MongoSettings(BaseSettings):
    mongo_details: str

class PostgresSettings(BaseSettings):
    users_database_url: str
    todo_database_url: str

class Settings(BaseSettings):
    mongo: MongoSettings = MongoSettings()
    postgres: PostgresSettings = PostgresSettings()

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


settings = Settings()
