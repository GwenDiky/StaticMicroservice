import logging
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List

import motor.motor_asyncio
from bson.errors import InvalidId
from bson.objectid import ObjectId
from pymongo.errors import OperationFailure

from src import exceptions
from src.core.config import setup_logging
from src.repositories.user_repo import AbstractStaticsRepository
from src.schemas.project_schema import ProjectStatisticSchema
from src.schemas.user_schema import UserStatisticSchema

setup_logging()


class StatusChoices(Enum):
    DONE = "Done"
    IN_PROGRESS = "In Progress"
    TO_DO = "To Do"


class UserStatisticService(AbstractStaticsRepository):
    def __init__(self, client: motor.motor_asyncio.AsyncIOMotorClient):
        self.client = client
        self.db = self.client.get_database("statics")
        self.collection = self.db["user_statics"]

    async def save_or_update_user_statistic(
        self, project_id: int, task_status: str
    ):
        try:
            logging.info(
                "Updating statistics for project %s with status %s",
                project_id,
                task_status,
            )

            project = await self.collection.find_one(
                {"project_id": project_id}
            )

            if not project:
                logging.info(
                    "New project statistics created for project_id %s",
                    project_id,
                )

                tasks_by_status = {"in_progress": 0, "done": 0, "to_do": 0}
                total_tasks = 1
                new_project_stat = ProjectStatisticSchema(
                    project_id=project_id,
                    total_tasks=total_tasks,
                    total_user=1,
                    tasks_by_status=tasks_by_status,
                    average_task_completion_time=0.0,
                )

                update_result = await self.collection.update_one(
                    {"project_id": project_id},
                    {"$setOnInsert": new_project_stat.dict()},
                    upsert=True,
                )

                if update_result.upserted_id:
                    logging.info(
                        "New project statistics created for project_id %s",
                        project_id,
                    )
                return

            tasks_by_status = project.get(
                "tasks_by_status", {"in_progress": 0, "done": 0, "to_do": 0}
            )
            total_tasks = project.get("total_tasks", 0) + 1

            if task_status in tasks_by_status:
                tasks_by_status[task_status] += 1
            else:
                logging.warning(
                    "Invalid task status: %s. Ignoring update.", task_status
                )
                raise exceptions.InvalidTaskStatusError

            average_task_completion_time = project.get(
                "average_task_completion_time", 0.0
            )

            updated_project_stat = ProjectStatisticSchema(
                project_id=project_id,
                total_tasks=total_tasks,
                total_user=project.get("total_user", 0),
                tasks_by_status=tasks_by_status,
                average_task_completion_time=average_task_completion_time,
            )

            update_result = await self.collection.update_one(
                {"project_id": project_id},
                {"$set": updated_project_stat.dict()},
            )

            if update_result.matched_count > 0:
                logging.info(
                    "Project statistics updated for project_id %s", project_id
                )
            else:
                logging.info(
                    "New project statistics created for project_id %s",
                    project_id,
                )

        except OperationFailure as e:
            logging.error("Error updating project statistic: %s", e)
            raise exceptions.DatabaseUpdateError

    async def get_user_statistic_by_id(
        self, static_id: str
    ) -> UserStatisticSchema:
        logging.info("Fetching user statistic for static: %s", static_id)

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

    async def get_all_user_statistics(self) -> list[UserStatisticSchema]:
        logging.info("Fetching all user statistics")
        documents = await self.collection.find().to_list(None)
        if documents:
            return [UserStatisticSchema(**doc) for doc in documents]
        raise exceptions.DocumentNotFoundException

    async def get_tasks_for_user(self, user_id) -> List[Dict]:
        tasks = await self.collection.find({"user_id": user_id}).to_list()
        return tasks

    async def calculate_total_projects(self, tasks: List[Dict]) -> int:
        project_ids = {
            task["project_id"] for task in tasks if "project_id" in task
        }
        return len(project_ids)

    async def calculate_tasks_completed_last_week(
        self, tasks: List[Dict]
    ) -> int:
        one_week_ago = datetime.now() - timedelta(days=7)
        completed_tasks = [
            task
            for task in tasks
            if task.get("status") == "done"
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
                task.get("status") in {StatusChoices.DONE, "Done"}
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
