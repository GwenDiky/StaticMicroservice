from abc import ABC, abstractmethod
from typing import Dict, List

from src.schemas.user_schema import UserStatisticSchema


class AbstractStaticsRepository(ABC):
    @abstractmethod
    async def delete_user_statics_for_task(self, project_id, task_id):
        raise NotImplementedError

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

    @abstractmethod
    async def calculate_total_projects(self, tasks: List[Dict]) -> int:
        raise NotImplementedError

    @abstractmethod
    async def calculate_tasks_completed_last_week(
        self, tasks: List[Dict]
    ) -> int:
        raise NotImplementedError

    @abstractmethod
    async def calculate_average_task_completion_time(
        self, tasks: List[Dict]
    ) -> float:
        raise NotImplementedError

    @abstractmethod
    async def save_or_update_user_static_new(
        self, user_id: int, project_id: int
    ):
        raise NotImplementedError
