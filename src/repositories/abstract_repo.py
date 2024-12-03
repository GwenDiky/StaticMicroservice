import motor.motor_asyncio
from src.core.config import settings
from bson.objectid import ObjectId
from src.schemas.user_schema import UserStatisticSchema
from fastapi import HTTPException, status
from pymongo.errors import DuplicateKeyError
import logging
from abc import ABC, abstractmethod

class AbstractStaticsRepository(ABC):
    @abstractmethod
    async def save_user_statistic(self, statistic: UserStatisticSchema):
        raise NotImplementedError

    @abstractmethod
    async def get_user_statistic_by_id(self,
                                       static_id: str) -> UserStatisticSchema:
        raise NotImplementedError

    @abstractmethod
    async def get_all_user_statistics(self) -> list[UserStatisticSchema]:
        raise NotImplementedError


