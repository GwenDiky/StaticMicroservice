import logging
from datetime import datetime
from collections import defaultdict
from typing import Optional

import motor.motor_asyncio
from pymongo.errors import OperationFailure
from fastapi import HTTPException

from src import exceptions
from src.repositories.project_repo import AbstractProjectStaticsRepository
from src.schemas.project_schema import ProjectStatisticSchema

from src.core.enums import StatusChoices

logger = logging.getLogger(__name__)


class ProjectStatisticService(AbstractProjectStaticsRepository):
    def __init__(self, client: motor.motor_asyncio.AsyncIOMotorClient):
        self.client = client
        self.db = self.client.get_database("statics")
        self.collection = self.db["project_statics"]

    async def delete_project_statistic_for_task(self, project_id, task_id):
        try:
            logger.info("Removing task %s from project %s", task_id,
                        project_id)
            project = await self.collection.find_one(
                {"project_id": project_id})

            if not project:
                logger.warning("Project with id %s not found", project_id)
                return

            update_result = await self.collection.update_one(
                {"project_id": project_id},
                {
                    "$unset": {f"tasks_by_status.{task_id}": ""},
                    "$inc": {"total_tasks": -1},
                },
            )

            if update_result.modified_count > 0:
                tasks_by_status = project.get("tasks_by_status", {})
                completed_tasks = await self.get_completed_tasks(project_id)
                average_task_completion_time = self.calculate_average_completion_time(
                    completed_tasks)

                tasks_by_status.pop(task_id, None)

                await self.collection.update_one(
                    {"project_id": project_id},
                    {
                        "$set": {
                            "tasks_by_status": tasks_by_status,
                            "average_task_completion_time": average_task_completion_time,
                        }
                    },
                )
            else:
                logger.warning(
                    "No changes made while removing task %s from project %s",
                    task_id,
                    project_id,
                )
        except OperationFailure as e:
            logger.error("Error updating project statistic: %s", e)
            raise exceptions.DatabaseUpdateError

    async def save_or_update_project_statistic(self, project_id: int,
                                               task_status: str):
        try:
            logger.info("Updating statistics for project %s with status %s",
                        project_id, task_status)

            project = await self.collection.find_one(
                {"project_id": project_id})

            if not project:
                await self.create_new_project_statistic(project_id,
                                                        task_status)
            else:
                await self.update_existing_project_statistic(project,
                                                             task_status)

        except OperationFailure as e:
            logger.error("Error updating project statistic: %s", e)
            raise HTTPException(status_code=500,
                                detail="Database update error")

    async def create_project_statistic(self, project_id: int,
                                           task_status: str):
        tasks_by_status = defaultdict(int)
        tasks_by_status[task_status] = 1

        statistic = ProjectStatisticSchema(
            project_id=project_id,
            total_tasks=1,
            total_user=1,
            tasks_by_status=dict(tasks_by_status),
            average_task_completion_time=0.0,
        )

        await self.collection.update_one(
            {"project_id": project_id},
            {"$setOnInsert": statistic.dict()},
            upsert=True,
        )

    async def update_existing_project_statistic(self, project:dict,
                                                task_status: str):
        tasks_by_status = project["tasks_by_status"]
        tasks_by_status[task_status] += 1
        total_tasks = project["total_tasks"] + 1

        completed_tasks = await self.collection.find(
            {"project_id": project["project_id"], "status": "done"}
        ).to_list(None)

        average_task_completion_time = self.calculate_average_completion_time(
            completed_tasks)

        logger.info("Tasks by statuses: %s", tasks_by_status)

        updated_project_stat = ProjectStatisticSchema(
            project_id=project["project_id"],
            total_tasks=total_tasks,
            total_user=project["total_user"],
            tasks_by_status=tasks_by_status,
            average_task_completion_time=average_task_completion_time,
        )

        await self.collection.update_one(
            {"project_id": project["project_id"]},
            {"$set": updated_project_stat.dict()},
        )

    async def get_completed_tasks(self, project_id: int) -> List[dict]:
        return await self.collection.find(
            {"project_id": project_id, "status": StatusChoices.DONE}
        ).to_list(None)

    async def calculate_average_completion_time(self, completed_tasks:
        List[dict]) -> float:
        total_time = 0
        for task in completed_tasks:
            if "created_at" in task and "updated_at" in task:
                total_time += (task["updated_at"] - task[
                    "created_at"]).total_seconds()

        return total_time / len(
            completed_tasks) if completed_tasks else 0.0
