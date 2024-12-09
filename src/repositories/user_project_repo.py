from abc import ABC, abstractmethod
from datetime import datetime


class AbstractUserProjectStaticsRepository(ABC):
    @abstractmethod
    async def update_task(self, user_id: int, project_id: int, status: str):
        raise NotImplementedError

    @abstractmethod
    async def get_completed_tasks_count(self, user_id: int, project_id: int,
                                        start_date: datetime,
                                        end_date: datetime):
        raise NotImplementedError

    @abstractmethod
    async def update_task_count(self, tasks_completed: int,
                          is_task_completed: bool) -> int:
        raise NotImplementedError

    @abstractmethod
    async def update_existing_stat(self, stat_id: str, tasks_completed: int,
                                   end_date: datetime):
        raise NotImplementedError

    @abstractmethod
    async def create_new_stat(self, user_id: int, project_id: int,
                              is_task_completed: bool):
        raise NotImplementedError

    @abstractmethod
    async def has_other_tasks_in_project(self, user_id: int,
                                         project_id: int) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def delete_user_project_statistic(self, user_id: int,
                                            project_id: int):
        raise NotImplementedError
