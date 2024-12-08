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
        is_task_completed = status.lower() == "done"
        end_date = datetime.now()
        start_date = end_date - timedelta(weeks=1)

        existing_stat = await self.collection.find_one(
            {"user_id": user_id, "project_id": project_id}
        )

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

        tasks_completed = (
            completed_tasks_last_week[0]["count"]
            if completed_tasks_last_week
            else 0
        )
        tasks_completed = (
            tasks_completed + 1
            if is_task_completed
            else max(0, tasks_completed - 1)
        )

        if existing_stat:
            await self.collection.update_one(
                {"_id": existing_stat["_id"]},
                {
                    "$set": {
                        "tasks_completed_last_week": tasks_completed,
                        "updated_at": end_date,
                    }
                },
            )
        else:
            new_stat = UserStaticProjectSchema(
                user_id=user_id,
                project_id=project_id,
                tasks_completed_last_week=1 if is_task_completed else 0,
            )
            await self.collection.insert_one(new_stat.dict())
