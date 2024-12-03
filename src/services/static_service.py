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

setup_logging()

class UserStatisticService(AbstractStaticsRepository):
    def __init__(self, client: motor.motor_asyncio.AsyncIOMotorClient):
        self.client = client
        self.db = self.client['statics']
        self.collection = self.db["user_statics"]

    # async def update_user_static_by_id(self):

    async def save_user_statistic(self, statistic: UserStatisticSchema):
        document = statistic.dict()
        document['_id'] = statistic.user_id
        logging.info(f"Attempting to save: {document}")

        try:
            result = await self.collection.insert_one(document)
            logging.info(f"User statistic saved with ID: {result.inserted_id}")
        except DuplicateKeyError:
            logging.warning(
                f"User statistic for user {statistic.user_id} already exists.")

    async def get_user_statistic_by_id(self,
                                       static_id: str) -> UserStatisticSchema:
        logging.info(f"Fetching user statistic for static: {static_id}")

        try:
            static_id_obj = ObjectId(static_id) if len(
                static_id) == 24 else static_id
        except InvalidId:
            raise exceptions.InvalidIdFormatException

        document = await self.collection.find_one({"_id": static_id_obj})

        if document:
            return UserStatisticSchema(**document)
        else:
            raise exceptions.DocumentNotFoundException

    async def get_all_user_statistics(self) -> list[UserStatisticSchema]:
        logging.info("Fetching all user statistics")
        documents = await self.collection.find().to_list(None)

        if documents:
            return [UserStatisticSchema(**doc) for doc in documents]
        else:
            raise exceptions.DocumentNotFoundException