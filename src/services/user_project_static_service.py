from datetime import datetime, timedelta

import motor.motor_asyncio

from src.repositories.user_project_repo import \
    AbstractUserProjectStaticsRepository
from src.schemas.user_project_schema import UserStaticProjectSchema


class UserProjectStatisticService(AbstractUserProjectStaticsRepository):
    def __init__(self, client: motor.motor_asyncio.AsyncIOMotorClient):
        self.client = client
        self.db = self.client.get_database("statics")
        self.collection = self.db["user_project_statics"]

    async def update_task(self, user_id: int, project_id: int, status: str):
        try:
            is_task_completed = status == "done"
            end_date = datetime.now()
            start_date = end_date - timedelta(weeks=1)

            existing_stat = await self.collection.find_one(
                {"user_id": user_id, "project_id": project_id}
            )

            tasks_completed = await self.get_completed_tasks_count(user_id,
                                                                   project_id,
                                                                   start_date,
                                                                   end_date)

            tasks_completed = self.update_task_count(tasks_completed,
                                                     is_task_completed)

            if existing_stat:
                await self.update_existing_stat(existing_stat["_id"],
                                                tasks_completed, end_date)
            else:
                await self.create_new_stat(user_id, project_id,
                                           is_task_completed)

        except OperationFailure as e:
            logger.error("Error updating task: %s", e)
            raise HTTPException(status_code=500,
                                detail="Database update error")

    async def get_completed_tasks_count(self, user_id: int, project_id: int,
                                        start_date: datetime,
                                        end_date: datetime):
        completed_tasks_last_week = await self.collection.aggregate(
            [
                {
                    "$match": {
                        "user_id": user_id,
                        "project_id": project_id,
                        "status": "done",
                    }
                },
                {
                    "$match": {
                        "updated_at": {"$gte": start_date, "$lt": end_date}
                    }
                },
                {"$group": {"_id": None, "count": {"$sum": 1}}},
            ]
        ).to_list(1)

        return completed_tasks_last_week[0][
            "count"] if completed_tasks_last_week else 0

    def update_task_count(self, tasks_completed: int,
                          is_task_completed: bool) -> int:
        return tasks_completed + 1 if is_task_completed else max(0,
                                                                 tasks_completed - 1)

    async def update_existing_stat(self, stat_id: str, tasks_completed: int,
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

    async def create_new_stat(self, user_id: int, project_id: int,
                              is_task_completed: bool):
        new_stat = UserStaticProjectSchema(
            user_id=user_id,
            project_id=project_id,
            tasks_completed_last_week=1 if is_task_completed else 0,
        )
        await self.collection.insert_one(new_stat.dict())

    async def has_other_tasks_in_project(self, user_id: int,
                                         project_id: int) -> bool:
        tasks = await self.collection.find(
            {"user_id": user_id, "project_id": project_id}).to_list(None)
        return len(tasks) > 0

    async def delete_user_project_statistic(self, user_id: int,
                                              project_id: int):
        remaining_stats = await self.collection.find(
            {"user_id": user_id}).to_list(None)

        if not remaining_stats:
            result = await self.collection.delete_one(
                {"user_id": user_id, "project_id": project_id})

            if result.deleted_count > 0:
                logger.info(
                    f"Deleted user statistics for user_id {user_id} and project_id {project_id}")
            else:
                logger.warning(
                    f"No statistics found to delete for user_id {user_id} and project_id {project_id}")
