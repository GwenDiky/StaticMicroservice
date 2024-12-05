import logging

import motor.motor_asyncio
from bson.errors import InvalidId
from bson.objectid import ObjectId
from pymongo.errors import OperationFailure

from src import exceptions
from src.core.config import setup_logging
from src.repositories.abstract_repo import AbstractStaticsRepository
from src.schemas.user_schema import UserStatisticSchema

setup_logging()


class UserStatisticService(AbstractStaticsRepository):
    def __init__(self, client: motor.motor_asyncio.AsyncIOMotorClient):
        self.client = client
        self.db = self.client.get_database("statics")
        self.collection = self.db["user_statics"]

    async def save_or_update_user_statistic(self, project_id, task_status):
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
                update_result = await self.collection.update_one(
                    {"project_id": project_id},
                    {
                        "$setOnInsert": {
                            "project_id": project_id,
                            "tasks_by_status": tasks_by_status,
                            "total_tasks": total_tasks,
                        }
                    },
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

            update_result = await self.collection.update_one(
                {"project_id": project_id},
                {
                    "$set": {
                        "total_tasks": total_tasks,
                        "tasks_by_status": tasks_by_status,
                    }
                },
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
