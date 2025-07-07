import logging
from datetime import datetime, timedelta
from typing import Optional

import motor.motor_asyncio
from fastapi import HTTPException
from pymongo.errors import OperationFailure

from src.core.enums import StatusChoices
from src.repositories.user_project_repo import \
    AbstractUserProjectStaticsRepository
from src.schemas.user_project_schema import UserStaticProjectSchema

logger = logging.getLogger(__name__)

class UserProjectStatisticService(AbstractUserProjectStaticsRepository):
    def __init__(self, client: motor.motor_asyncio.AsyncIOMotorClient):
        self.client = client
        self.db = self.client.get_database("statics")
        self.collection = self.db["user_project_statics"]

    @staticmethod
    async def get_last_week_dates() -> tuple:
        end_date = datetime.now()
        start_date = end_date - timedelta(weeks=1)
        return start_date, end_date

    async def is_statistic_exists(self, user_id, project_id):
        return await self.collection.find_one(
            {"user_id": user_id, "project_id": project_id}
        )

    async def update_statistics(self, user_id: int, project_id: int, status:
    str):
        try:
            is_task_completed = status == StatusChoices.DONE
            end_date, start_date = (
                UserProjectStatisticService.get_last_week_dates()
            )

            tasks_completed = await self.get_completed_tasks_count(user_id, project_id, start_date, end_date)

            tasks_completed = self.update_task_count(tasks_completed, is_task_completed)

            if await self.is_statistic_exists(user_id=user_id,
                                                 project_id=project_id):
                existing_statistic = await self.collection.find_one(
                    {"user_id": user_id, "project_id": project_id})
                await self.update_existing_entry(existing_statistic["_id"],
                                              tasks_completed, end_date)
            else:
                await self.create_statistic_entry(user_id, project_id,
                                            is_task_completed)

        except OperationFailure as e:
            logger.error("Error updating task: %s", e)
            raise HTTPException(status_code=500, detail="Database update error")

    async def get_completed_tasks_count(self, user_id: int, project_id: int, start_date: datetime, end_date: datetime) -> int:
        completed_tasks_last_week = await self.collection.aggregate(
            [
                {
                    "$match": {
                        "user_id": user_id,
                        "project_id": project_id,
                        "status": "done",
                        "updated_at": {"$gte": start_date, "$lt": end_date}
                    }
                },
                {"$group": {"_id": None, "count": {"$sum": 1}}}
            ]
        ).to_list(1)

        return completed_tasks_last_week[0]["count"] if completed_tasks_last_week else 0

    def update_task_count(self, tasks_completed: int, is_task_completed: bool) -> int:
        return tasks_completed + 1 if is_task_completed else max(0, tasks_completed - 1)

    async def update_existing_entry(self, stat_id: str, tasks_completed: int,
                            end_date: datetime):
        await self.collection.update_one(
            {"_id": stat_id},
            {
                "$set": {
                    "tasks_completed_last_week": tasks_completed,
                    "updated_at": end_date,
                }
            },
        )

    async def create_statistic_entry(self, user_id: int, project_id: int,
                               is_task_completed: bool):
        statistic = UserStaticProjectSchema(
            user_id=user_id,
            project_id=project_id,
            tasks_completed_last_week=1 if is_task_completed else 0,
        )
        await self.collection.insert_one(statistic.dict())

    async def has_other_tasks_in_project(self, user_id: int, project_id: int) -> bool:
        count = await self.collection.count_documents(
            {"user_id": user_id, "project_id": project_id}
        )
        return count > 0

    async def delete_user_project_statistic(self, user_id: int, project_id: int):
        remaining_statistic = await self.has_other_tasks_in_project(user_id,
                                                                project_id)

        if not remaining_statistic:
            result = await self.collection.delete_one(
                {"user_id": user_id, "project_id": project_id}
            )

            if result.deleted_count > 0:
                logger.info(f"Deleted user statistics for user_id {user_id} and project_id {project_id}")
            else:
                logger.warning(f"No statistics found to delete for user_id {user_id} and project_id {project_id}")
        else:
            logger.info(f"Other tasks exist for user_id {user_id} in project_id {project_id}, skipping deletion")

    async def get_user_project_statistic(self, user_id: int, project_id: int) -> Optional[UserStaticProjectSchema]:
        stat = await self.collection.find_one(
            {"user_id": user_id, "project_id": project_id}
        )
        if stat:
            return UserStaticProjectSchema(**stat)
        return None
