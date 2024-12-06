import motor.motor_asyncio

from src.repositories.user_project_repo import \
    AbstractUserProjectStaticsRepository


class UserProjectStatisticService(AbstractUserProjectStaticsRepository):
    def __init__(self, client: motor.motor_asyncio.AsyncIOMotorClient):
        self.client = client
        self.db = self.client.get_database("statics")
        self.collection = self.db["user_project_statics"]
