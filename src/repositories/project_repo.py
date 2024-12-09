from abc import ABC, abstractmethod


class AbstractProjectStaticsRepository(ABC):
    @abstractmethod
    async def delete_user_statics_for_task(self, project_id, task_id):
        raise NotImplementedError

    @abstractmethod
    async def save_or_update_project_statistic(self, project_id: int,
        task_status: str):
        raise NotImplementedError

    @abstractmethod
    async def create_new_project_statistic(self, project_id: int,
                                           task_status: str):
        raise NotImplementedError

    @abstractmethod
    async def update_existing_project_statistic(self, project,
                                                task_status: str):
        raise NotImplementedError

    @abstractmethod
    async def calculate_average_completion_time(self, completed_tasks):
        raise NotImplementedError