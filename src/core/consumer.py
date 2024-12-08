import logging

import motor.motor_asyncio

from src.core.config import setup_logging
from src.core.kafka_consumer import KafkaConsumer
from src.services.project_static_service import ProjectStatisticService
from src.services.user_project_static_service import \
    UserProjectStatisticService
from src.services.user_static_service import UserStatisticService

setup_logging()
logger = logging.getLogger(__name__)


async def start_listen(client: motor.motor_asyncio.AsyncIOMotorClient):
    user_statistic_service = UserStatisticService(client)
    project_static_service = ProjectStatisticService(client)
    user_project_static_service = UserProjectStatisticService(client)
    kafka_consumer = KafkaConsumer(
        user_db=user_statistic_service,
        project_db=project_static_service,
        user_project_db=user_project_static_service,
    )
    await kafka_consumer.consume_messages()
