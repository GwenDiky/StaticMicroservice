from src.core.config import settings
import motor.motor_asyncio
from src.core.config import settings
from bson.objectid import ObjectId
from src.schemas.user_schema import UserStatisticSchema
from fastapi import HTTPException, status
from pymongo.errors import DuplicateKeyError
import logging
from src.core.config import setup_logging
from src.repositories.abstract_repo import AbstractStaticsRepository
from bson.errors import InvalidId
from src import exceptions

def get_mongo_client() -> motor.motor_asyncio.AsyncIOMotorClient:
    client = motor.motor_asyncio.AsyncIOMotorClient(settings.mongo.mongo_details)
    try:
        yield client
    finally:
        client.close()
