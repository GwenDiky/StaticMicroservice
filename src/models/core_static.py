import motor.motor_asyncio
from fastapi import Depends

from src.core.config import settings
from src.services import user_static_service as static_service


def get_mongo_client() -> motor.motor_asyncio.AsyncIOMotorClient:
    client = motor.motor_asyncio.AsyncIOMotorClient(
        settings.mongo.mongo_details
    )
    return client


def get_user_statistic_service(
    client: motor.motor_asyncio.AsyncIOMotorClient = Depends(get_mongo_client),
):
    return static_service.UserStatisticService(client)
