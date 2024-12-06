import motor.motor_asyncio

from src.repositories.projects_repo import AbstractProjectStaticsRepository


class ProjectStatisticService(AbstractProjectStaticsRepository):
    def __init__(self, client: motor.motor_asyncio.AsyncIOMotorClient):
        self.client = client
        self.db = self.client.get_database("statics")
        self.collection = self.db["project_statics"]
