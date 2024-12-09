import asyncio
import json
import logging
from typing import Optional

from aiokafka import AIOKafkaConsumer, KafkaError
import aiokafka

logger = logging.getLogger(__name__)


class KafkaConsumer:
    def __init__(
        self,
        user_db,
        project_db,
        user_project_db,
        bootstrap_servers="kafka:9092",
        group_id="task-consumer-group",
        topic="tasks",
    ):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic = topic
        self.user_db = user_db
        self.project_db = project_db
        self.user_project_db = user_project_db
        self.consumer: Optional[AIOKafkaConsumer] = None

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            self.topic,
            loop=asyncio.get_event_loop(),
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="earliest",
        )
        await self.consumer.start()
        logger.info("Kafka consumer started")

    async def consume_messages(self):
        logger.debug(
            "Starting to consume messages from Kafka topic: %s", self.topic
        )
        try:
            async for message in self.consumer:
                logger.info(
                    "Consumed message: %s from offset %d",
                    message.value.decode("utf-8"),
                    message.offset,
                )
                await self.process_message(message)
        except aiokafka.errors.NotCoordinatorForGroupError:
            logger.error("Coordinator for group not found, retrying...")
            await asyncio.sleep(5)
        except aiokafka.errors.KafkaConnectionError as e:
            logger.error("Connection error: %s, retrying...", e)
            await asyncio.sleep(5)

    async def stop(self):
        if self.consumer:
            await self.consumer.stop()
            logger.info("Kafka consumer stopped")

    async def process_message(self, msg: aiokafka.ConsumerRecord) -> None:
        try:
            logger.debug("Starting message processing")
            message = msg.value.decode("utf-8")
            event = json.loads(message)
            logger.info("Processed event: %s", event)

            event_type = event.get("event_type")
            task_data = event.get("task_data")

            if task_data:
                logger.info("Received task_data: %s", task_data)
            else:
                logger.info("Didn't receive task data")

            project_id = task_data.get("project_id")
            task_status = task_data.get("status")
            task_user_id = task_data.get("user_id")

            if (
                project_id is None
                or task_status is None
                or task_user_id is None
            ):
                logger.error(
                    "Missing required fields in task_data: %s", task_data
                )
                return

            logger.info("Received user_id: %s", task_user_id)

            await self.update_statistics(
                project_id=project_id,
                task_status=task_status,
                task_user_id=task_user_id,
                event_type=event_type,
                task_data=task_data,
            )

        except json.JSONDecodeError as e:
            logger.error("JSON decode error: %s", e)
        except KafkaError as e:
            logger.error("Kafka error: %s", e)

    async def update_statistics(
        self,
        project_id: int,
        task_status: str,
        task_user_id: int,
        event_type: str,
        task_data,
    ):
        if event_type in ["task_created", "task_updated"]:
            logger.info(
                "Updating statistics for project_id: %s, status: %s",
                project_id,
                task_status,
            )
            await self.user_db.save_or_update_user_statistic(
                project_id, task_status
            )
            await self.project_db.save_or_update_project_statistic(
                project_id=project_id, task_status=task_status
            )
            await self.user_project_db.update_statistics(
                user_id=task_user_id, project_id=project_id, status=task_status
            )
        elif event_type == "task_deleted":
            logger.info(
                "Deleting statistics for task_id: %s", task_data["task_id"]
            )
            await self.delete_statistics(
                project_id, task_data["task_id"], task_user_id
            )

    async def delete_statistics(
        self, project_id: int, task_id: str, task_user_id: int
    ):
        await self.project_db.delete_project_statistic_for_task(
            project_id, task_id
        )
        await self.user_db.delete_user_statistic(
            project_id=project_id, task_status=task_status
        )
        await self.user_project_db.delete_user_project_statistic(
            user_id=task_user_id, project_id=project_id
        )
