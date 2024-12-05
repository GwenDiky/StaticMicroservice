from abc import ABC, abstractmethod

from src.schemas.user_schema import UserStatisticSchema


class AbstractStaticsRepository(ABC):
    @abstractmethod
    async def save_or_update_user_statistic(self, project_id, task_status):
        raise NotImplementedError

    @abstractmethod
    async def get_user_statistic_by_id(
        self, static_id: str
    ) -> UserStatisticSchema:
        raise NotImplementedError

    @abstractmethod
    async def get_all_user_statistics(self) -> list[UserStatisticSchema]:
        raise NotImplementedError
