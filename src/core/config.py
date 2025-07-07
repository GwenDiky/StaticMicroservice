import logging
import sys

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
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)


settings = Settings()
