import asyncio
import json
import logging

import aiokafka
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

logger = logging.getLogger(__name__)


class KafkaConsumer:
    def __init__(
        self,
        db,
        bootstrap_servers="kafka:9092",
        group_id="task-consumer-group",
        topic="tasks",
    ):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic = topic
        self.db = db

    async def consume_messages(self):
        logger.debug(
            "Starting to consume messages from Kafka topic: %s", self.topic
        )

        while True:
            try:
                consumer = AIOKafkaConsumer(
                    self.topic,
                    loop=asyncio.get_event_loop(),
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=self.group_id,
                    auto_offset_reset="earliest",
                )

                await consumer.start()

                async for message in consumer:
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
            finally:
                await consumer.stop()

    async def process_message(self, msg):
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
                logger.info("Didn't receive task data'")

            project_id = task_data.get("project_id")
            task_status = task_data.get("status")
            task_user_id = task_data.get("user_id")

            if task_user_id is None:
                logging.error("Missing user_id in message: %s", event)
                return
            logger.info("Received user_id: %s", task_user_id)

            if event_type in ["task_created", "task_updated"]:
                logger.info(
                    "Updating statistics for project_id: %s, status: %s",
                    project_id,
                    task_status,
                )
                await self.db.save_or_update_user_statistic(
                    project_id, task_status
                )
                await self.db.save_or_update_user_static_new(
                    task_user_id, project_id
                )
                logger.info(
                    "Statistics updated for project_id: %s", project_id
                )

            elif event_type == "task_deleted":
                logger.info(
                    "Deleting static for task_id: %s", task_data["task_id"]
                )
                await self.db.delete_user_statics_for_task(
                    project_id, task_data["task_id"]
                )
                logger.info(
                    "Statics deleted for task_id: %s", task_data["task_id"]
                )

        except json.JSONDecodeError as e:
            logger.error("JSON decode error: %s", e)
        except KafkaError as e:
            logger.error("Kafka error: %s", e)
