import logging

import motor.motor_asyncio

from src.core.config import setup_logging
from src.core.kafka_consumer import KafkaConsumer
from src.services.user_static_service import UserStatisticService

setup_logging()
logger = logging.getLogger(__name__)


async def start_listen(client: motor.motor_asyncio.AsyncIOMotorClient):
    user_statistic_service = UserStatisticService(client)
    kafka_consumer = KafkaConsumer(db=user_statistic_service)
    await kafka_consumer.consume_messages()
