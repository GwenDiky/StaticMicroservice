import logging
from datetime import datetime, timedelta
from typing import Dict, List

import motor.motor_asyncio
from bson.errors import InvalidId
from bson.objectid import ObjectId
from fastapi import HTTPException
from pymongo.errors import OperationFailure

from src import exceptions
from src.core.enums import StatusChoices
from src.repositories.user_repo import AbstractStaticsRepository
from src.schemas.project_schema import ProjectStatisticSchema
from src.schemas.user_schema import UserStatisticSchema

logger = logging.getLogger(__name__)


class UserStatisticService(AbstractStaticsRepository):
    DEFAULT_TASKS_BY_STATUS = {
        StatusChoices.DONE: 0,
        StatusChoices.IN_PROGRESS: 0,
        StatusChoices.TO_DO: 0,
    }

    def __init__(self, client: motor.motor_asyncio.AsyncIOMotorClient):
        self.client = client
        self.db = self.client.get_database("statics")
        self.collection = self.db["user_statics"]

    async def save_or_update_user_statistic(
        self, project_id: int, task_status: str
    ):
        try:
            logger.info(
                "Updating statistics for project %s with status %s",
                project_id,
                task_status,
            )
            project = await self.collection.find_one(
                {"project_id": project_id}
            )

            if not project:
                await self.create_project_statistic(project_id)
            else:
                await self.update_existing_project_statistic(
                    project, task_status, project_id
                )

        except OperationFailure as e:
            logger.error("Error updating project statistic: %s", e)
            raise HTTPException(
                status_code=500, detail="Database update error"
            )

    async def create_project_statistic(self, project_id: int):
        logger.info(
            "New project statistics created for project_id %s", project_id
        )

        tasks_by_status = UserStatisticService.DEFAULT_TASKS_BY_STATUS.copy()
        statistic = ProjectStatisticSchema(
            project_id=project_id,
            total_tasks=1,
            total_user=1,
            tasks_by_status=tasks_by_status,
            average_task_completion_time=0.0,
        )

        await self._upsert_project_statistic(
            project_id=project_id, project_stat=statistic
        )

    async def _upsert_project_statistic(
        self, project_id: int, project_stat: ProjectStatisticSchema
    ):
        update_result = await self.collection.update_one(
            {"project_id": project_id},
            {"$set": project_stat.dict()},
            upsert=True,
        )

        if update_result.upserted_id or update_result.matched_count > 0:
            logger.info(
                "Project statistics updated for project_id %s", project_id
            )

    async def update_existing_project_statistic(
        self, project, task_status: str, project_id: int
    ):
        tasks_by_status = project.get(
            "tasks_by_status",
            UserStatisticService.DEFAULT_TASKS_BY_STATUS.copy(),
        )
        total_tasks = project.get("total_tasks", 0) + 1

        if task_status in tasks_by_status:
            tasks_by_status[task_status] += 1
        else:
            logger.warning(
                "Invalid task status: %s. Ignoring update.", task_status
            )
            raise exceptions.InvalidTaskStatusError

        average_task_completion_time = project.get(
            "average_task_completion_time", 0.0
        )

        statistic = ProjectStatisticSchema(
            project_id=project_id,
            total_tasks=total_tasks,
            total_user=project.get("total_user", 0),
            tasks_by_status=tasks_by_status,
            average_task_completion_time=average_task_completion_time,
        )

        await self._upsert_project_statistic(
            project_id=project_id, project_stat=statistic
        )

    async def get_user_statistic_by_id(
        self, static_id: str
    ) -> UserStatisticSchema:
        logger.info("Fetching user statistic for static: %s", static_id)

        try:
            static_id_obj = (
                ObjectId(static_id) if len(static_id) == 24 else static_id
            )
        except InvalidId as exc:
            raise exceptions.InvalidIdFormatException from exc

        document = await self.collection.find_one({"_id": static_id_obj})

        if document:
            return UserStatisticSchema(**document)
        raise exceptions.DocumentNotFoundException

    async def get_all_user_statistics(self) -> List[UserStatisticSchema]:
        logger.info("Fetching all user statistics")
        documents = await self.collection.find().to_list(None)
        if documents:
            return [UserStatisticSchema(**doc) for doc in documents]
        raise exceptions.DocumentNotFoundException

    async def get_tasks_for_user(self, user_id: str) -> List[Dict]:
        tasks = await self.collection.find({"user_id": user_id}).to_list(None)
        return tasks

    async def calculate_total_projects(self, tasks: List[Dict]) -> int:
        return len(
            {task["project_id"] for task in tasks if "project_id" in task}
        )

    async def calculate_tasks_completed_last_week(
        self, tasks: List[Dict]
    ) -> int:
        one_week_ago = datetime.now() - timedelta(days=7)
        completed_tasks = [
            task
            for task in tasks
            if task.get("status") == StatusChoices.DONE
            and task.get("updated_at") >= one_week_ago
        ]
        return len(completed_tasks)

    async def calculate_average_task_completion_time(
        self, tasks: List[Dict]
    ) -> float:
        completed_tasks = [
            task
            for task in tasks
            if (
                task.get("status") == StatusChoices.DONE
                and isinstance(task.get("created_at"), datetime)
                and isinstance(task.get("updated_at"), datetime)
            )
        ]

        if not completed_tasks:
            return 0.0

        total_time = sum(
            (task["updated_at"] - task["created_at"]).total_seconds()
            for task in completed_tasks
        )

        return total_time / len(completed_tasks)

    async def delete_user_statistic(self, project_id: int, task_status: str):
        try:
            logging.info(
                "Handling task deletion for project %s with status %s",
                project_id,
                task_status,
            )

            project = await self.collection.find_one(
                {"project_id": project_id}
            )

            if not project:
                logging.warning(
                    "No statistics found for project_id %s", project_id
                )
                return

            tasks_by_status = project.get(
                "tasks_by_status",
                UserStatisticService.DEFAULT_TASKS_BY_STATUS.copy(),
            )
            total_tasks = max(project.get("total_tasks", 0) - 1, 0)

            average_task_completion_time = project.get(
                "average_task_completion_time", 0.0
            )

            if task_status == StatusChoices.DONE:
                tasks = await self.get_tasks_for_project(project_id)
                average_task_completion_time = (
                    await self.calculate_average_task_completion_time(tasks)
                )

            statistic = ProjectStatisticSchema(
                project_id=project_id,
                total_tasks=total_tasks,
                total_user=project.get("total_user", 0),
                tasks_by_status=tasks_by_status,
                average_task_completion_time=average_task_completion_time,
            )

            await self._upsert_project_statistic(
                project_id, project_stat=statistic
            )

        except OperationFailure as exc:
            logging.error(
                "Error handling task deletion for project_id %s: %s",
                project_id,
                exc,
            )
            raise exceptions.DatabaseUpdateError from exc

    async def get_tasks_for_project(self, project_id: int) -> list:
        tasks = await self.collection.find(
            {"project_id": project_id}
        ).to_list()
        return tasks
